#ifndef QUEUE_RACE_STORAGE_H
#define QUEUE_RACE_STORAGE_H

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <cerrno>
#include <mutex>
#include <thread>

#include "cache/concurrent-scalable-cache.h"
#include "common.h"

namespace race2018 {

/* -----------------------------------------------
 * MemBlock for messages.
 ------------------------------------------------*/
struct MemBlock {
    void* ptr;    // new char[length]
    size_t size;  // length of msg
};
using Message = MemBlock;
static MemBlock new_memblock(const char* src, size_t size) {
    MemBlock msg;
    msg.ptr = ::malloc(size);
    ::memcpy(msg.ptr, src, size);
    msg.size = size;
    return msg;
}
static inline void free_message(const Message& msg) { ::free(msg.ptr); }

/* -----------------------------------------------
 * Functions to read/write on buffers.
 ------------------------------------------------*/

namespace buffer {
template <class T>
union ReadWriter {
    T t;
    uint8_t bytes[sizeof(T)];
};

// Developers must be careful when using the following helper functions
// that T must be a simple struct and src/dest buffers must have enough
// bytes to read/write, otherwise behaviour is undefined.

template <class T>
void read_from_buf(const void* src, T& data) {
    ReadWriter<T>* reader = reinterpret_cast<ReadWriter<T>*>(&data);
    ::memcpy(reader->bytes, src, sizeof(T));
}

template <class T>
void write_to_buf(void* dest, const T& data) {
    ReadWriter<T>* writer = reinterpret_cast<ReadWriter<T>*>(dest);
    writer->t = data;
}

template <class T>
using RefHandleFunc = std::function<void(const T&)>;

template <class T>
void read_and_handle(const void* src, const RefHandleFunc<T>& handle_func) {
    ReadWriter<T>* reader = reinterpret_cast<ReadWriter<T>*>(src);
    handle_func(reader->t);
}

template <class T>
void batch_read_and_handle(const void* src, size_t size, const RefHandleFunc<T>& handle_func) {
    ReadWriter<T>* reader;
    uint8_t* ptr = (uint8_t*)src;
    for (size_t i = 0; i < size; ++i, ptr += sizeof(T)) {
        reader = reinterpret_cast<ReadWriter<T>*>(ptr);
        handle_func(reader->t);
    }
}
}  // namespace buffer

/* -----------------------------------------------
 * Helper class for stepped value generate
 ------------------------------------------------*/

template <class T, typename = std::enable_if<std::is_integral<T>::value>>
class SteppedValue {
public:
    SteppedValue(T step) : SteppedValue(T{}, step) {}
    SteppedValue(T value, T step) : value_(value), step_(step) {}
    T next() { return value_.fetch_add(step_); }
    T next(size_t n) { return value_.fetch_add(n * step_); }
    T next_raw(size_t raw_n) { return value_.fetch_add(raw_n); }

private:
    const T step_;
    Atomic<T> value_;
};

/* -----------------------------------------------
 * File page and paged files, provides useful macros
 * and structs to operate file page.
 ------------------------------------------------*/
#define KILO_BYTES(n) (uint64_t(n) << 10)
#define MEGA_BYTES(n) (uint64_t(n) << 20)
#define GIGA_BYTES(n) (uint64_t(n) << 30)
#define TERA_BYTES(n) (uint64_t(n) << 40)
// must be power of 2
#define FILE_PAGE_SIZE KILO_BYTES(4)

// File page header
struct __attribute__((__packed__)) FilePageHeader {
    uint64_t offset;
};

#define FILE_PAGE_HEADER_SIZE sizeof(FilePageHeader)
#define FILE_PAGE_AVALIABLE_SIZE (FILE_PAGE_SIZE - FILE_PAGE_HEADER_SIZE)

// File page
struct __attribute__((__packed__)) FilePage {
    FilePageHeader header;
    char content[FILE_PAGE_AVALIABLE_SIZE];
};
#define FILE_PAGE_SLOT_OFFSET_OF(offset) (offset & (FILE_PAGE_SIZE - 1) - FILE_PAGE_HEADER_SIZE)
#define FILE_PAGE_OFFSET_OF(offset) (offset - (offset & (FILE_PAGE_SIZE - 1))
#define FILE_PAGE_OF_PTR(ptr) ((FilePage*)ptr)
#define FILE_OFFSET_OF(page_offset, slot_offset) (page_offset + FILE_PAGE_HEADER_SIZE + slot_offset)
#define NEGATIVE_OFFSET uint64_t(-1LL)
#define PAGE_OFFSET_LIMIT TERA_BYTES(1)

extern void* map_file_page(int fd, size_t offset, bool readonly);
extern void unmap_file_page(void* addr);
class MappedFilePagePtr {
public:
    MappedFilePagePtr(void* addr) : file_page_((FilePage*)addr) {}
    MappedFilePagePtr(int fd, size_t offset, bool readonly) {
        file_page_ = (FilePage*)map_file_page(fd, offset, readonly);
    }
    ~MappedFilePagePtr() { unmap_file_page(file_page_); }
    FilePage& operator*() const noexcept { return *file_page_; }
    FilePage* operator->() const noexcept { return file_page_; }
    operator bool() const noexcept { return file_page_ != nullptr; }
    bool empty() const noexcept { return file_page_ == nullptr; }
    FilePage* get() noexcept { return file_page_; }
    MappedFilePagePtr& operator=(const MappedFilePagePtr&) = delete;

private:
    FilePage* file_page_ = nullptr;
};

class PagedFile {
public:
    PagedFile(const String& file);
    ~PagedFile();

    int fd() const { return fd_; }
    const String& get_file() const { return this->file_; }
    size_t size() const { return this->size_; }
    void read(uint64_t offset, const FilePageReader& reader);
    void read(uint64_t offset, FilePage* page);
    void write(uint64_t offset, const FilePageWriter& writer);
    void write(uint64_t offset, const FilePage* page);
    void write(uint64_t offset, const char* buf, size_t size);
    void read(uint64_t offset, char* buf, size_t size);
    void write(const char* buf, size_t size);
    void read(char* buf, size_t size);
    void mapped_read(uint64_t offset, const FilePageReader& reader);
    void mapped_write(uint64_t offset, const FilePageWriter& writer);
    uint64_t allocate_block(size_t size) { return page_offset.next_raw(size); }

    uint64_t tls_write(const FilePage* page);
    void tls_flush();
    struct AsyncReadRequset {
        uint32_t page_idx;
        ConcurrentBoundedQueue<FilePage*>* cb_queue;
    };
    bool async_read(const AsyncReadRequset& request);

protected:
    std::thread* async_reader_ = nullptr;
    ConcurrentBoundedQueue<AsyncReadRequset> async_read_requests_;
    void start_async_reader();

    SteppedValue<uint64_t> page_offset{FILE_PAGE_SIZE};

private:
    int fd_ = -1;
    size_t size_ = 0;
    String file_;
};

#define TLS_WRITE_BUFFER_SIZE MEGA_BYTES(32)
#define TLS_WRITE_BUFFER_PAGE_SIZE (TLS_WRITE_BUFFER_SIZE / FILE_PAGE_SIZE)
// TLS write buffer will flush themself on destruction
class TLSWriteBuffer {
public:
    TLSWriteBuffer(PagedFile* file);
    ~TLSWriteBuffer();
    uint64_t write(FilePage* page);
    void flush();
    void allocate_offset();

private:
    char buf_[TLS_WRITE_BUFFER_SIZE];
    std::mutex mutex_;
    uint16_t volatile page_count_ = 0;
    uint16_t file_offset_;
    PagedFile* file_;
}

/* -----------------------------------------------
 * MessageQueue and QueueStore, to divide the storage
 * into individual queues.
 ------------------------------------------------*/

class QueueStore;
struct __attribute__((__packed__)) MessagePageIndex {
    uint32_t volatile page_idx;
    uint32_t prev_total_msg_size;
    uint16_t msg_size = 0;
    uint8_t volatile file_idx;
    MessagePageIndex() {}
    MessagePageIndex(uint32_t page_idx, uint32_t prev_total_msg_size)
        : page_idx(page_idx), prev_total_msg_size(prev_total_msg_size) {}
    uint32_t total_msg_size() const { return prev_total_msg_size + msg_size; }
};

class MessageQueue {
    friend class QueueStore;

public:
    explicit MessageQueue();
    explicit MessageQueue(uint32_t queue_id, QueueStore* store);

    uint32_t get_queue_id() const { return this->queue_id_; }

    void put(const MemBlock& message);
    Vector<MemBlock> get(uint32_t offset, uint32_t number);

protected:
    void read_msgs(const MessagePageIndex& index, uint32_t& offset, uint32_t& num, const char* ptr,
                   Vector<MemBlock>& msgs);
    size_t binary_search_indices(uint32_t msg_offset) const;
    // Methods for message slots in data page, true indicates page is full
    // and a new page's needed
    bool next_message_slot(uint16_t& slot_offset, uint16_t size);
    // Methods for write/flushing data
    void write_to_last_page(const MemBlock& msg, uint16_t slot_offset);
    void write_to_last_page(const char* ptr, size_t size, uint16_t slot_offset);
    // allocate next page, flush last page in last_page_ and set index
    void flush_last_page(bool release);
    // Methods for QueueStore to initialize id and name when it
    // create a new queue.
    void set_queue_id(uint32_t queue_id) { this->queue_id_ = queue_id; }
    // Methods for extracting message length
    uint16_t extract_message_length(const char*& ptr);

    // Struct and methods for save/load metadata of this queue
    struct Metadata;
    void construct_metadata(const String& queue_name, Metadata& metadata) const;
    void flush_queue_metadata(const String& queue_name, int fd) const;
    String load_queue_metadata(const Metadata& metadata, const char* buf);

private:
    uint32_t queue_id_ = -1;

    // Cursors for writing data
    uint16_t cur_data_slot_off_ = 0;

    // Write buffer
    std::mutex wq_mutex_;
    FilePage* last_page_;

    // Paged message index
    Vector<MessagePageIndex> paged_message_indices_;

    // QueueStore
    QueueStore* store_;
};

#define DATA_FILE_SPLITS 10

class QueueStore {
public:
    QueueStore(const String& location);
    ~QueueStore();

    void put(const String& queue_name, const MemBlock& message);
    Vector<MemBlock> get(const String& queue_name, long offset, long size);

    String data_file_path(int idx) const {
        return location_ + "/messages_" + std::to_string(idx) + ".data";
    }
    String index_file_path() const { return location_ + "/index.data"; }

    int8_t tls_data_file_idx();
    PagedFile* get_data_file();

protected:
    static thread_local int8_t file_idx;
    Atomic<int8_t> next_data_file_idx_{0};

protected:
    ConcurrentHashMap<String, SharedPtr<MessageQueue>> queues_{1000010};

    SharedPtr<MessageQueue> find_or_create_queue(const String& queue_name);
    SharedPtr<MessageQueue> find_queue(const String& queue_name) const;

protected:
    std::mutex flush_mutex_;
    bool volatile flushed = false;
    void flush_all_before_read();

private:
    String location_;
    PagedFile* data_files_[DATA_FILE_SPLITS];
    SteppedValue<uint32_t> next_queue_id_{1};
};

}  // namespace race2018

#endif  // QUEUE_RACE_STORAGE_H
