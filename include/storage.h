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
#define FILE_PAGE_SIZE KILO_BYTES(2)

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

using FilePageReader = std::function<void(const char*)>;
using FilePageWriter = std::function<void(char*)>;
#define PAGED_FILE_WRITE_BUFFER_SIZE MEGA_BYTES(32)
#define PAGES_IN_WRITE_BUFFER (PAGED_FILE_WRITE_BUFFER_SIZE / FILE_PAGE_SIZE)
class PagedFile {
    struct WriteBuffer {
        char buf[PAGED_FILE_WRITE_BUFFER_SIZE];
        uint64_t current_pages = 0;
        uint64_t start_offset = NEGATIVE_OFFSET;
        PagedFile* volatile file = nullptr;
        ~WriteBuffer() {
            if (file) file->flush(this);
        }
    };
    static thread_local bool write_buffer_initialized_;
    static thread_local SharedPtr<WriteBuffer> tls_write_buffer_;
    ConcurrentBoundedQueue<SharedPtr<WriteBuffer>> write_buffer_queue_;
    Atomic<uint64_t> next_wbq_{0};
    ConcurrentBoundedQueue<SharedPtr<WriteBuffer>> wbq_[2];

public:
    void clear_and_disable_write_buffer() {
        write_buffer_queue_.set_capacity(0);
        write_buffer_queue_.clear();
    }

    void write_out_all_buffers() {
        for (int i = 0; i < 2; ++i) {
            SharedPtr<WriteBuffer> wb;
            while (wbq_[i].try_pop(wb)) {
                flush(wb.get());
            }
        }
    }

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
    // this is thread local buffered write
    uint64_t async_write(const FilePage* page);
    void mapped_read(uint64_t offset, const FilePageReader& reader);
    void mapped_write(uint64_t offset, const FilePageWriter& writer);
    void flush(WriteBuffer* write_buf);
    void async_flush();
    uint64_t allocate_block(size_t size) { return page_offset.next_raw(size); }
#ifdef __linux__
    // use this carefully, because read throughput is not large and memory is
    // not large enough
    void readahead(uint64_t offset) { ::readahead(fd_, offset, FILE_PAGE_SIZE); }
#endif

protected:
    SteppedValue<uint64_t> page_offset{FILE_PAGE_SIZE};

private:
    int fd_ = -1;
    size_t size_ = 0;
    String file_;
};

/* -----------------------------------------------
 * MessageQueue and QueueStore, to divide the storage
 * into individual queues.
 ------------------------------------------------*/

class QueueStore;
struct __attribute__((__packed__)) MessagePageIndex {
    uint32_t volatile page_idx;
    uint32_t prev_total_msg_size;
    uint16_t msg_size = 0;
    MessagePageIndex() {}
    MessagePageIndex(uint32_t page_idx, uint32_t prev_total_msg_size)
        : page_idx(page_idx), prev_total_msg_size(prev_total_msg_size) {}
    uint32_t total_msg_size() const { return prev_total_msg_size + msg_size; }
};

class MessageQueue {
    friend class QueueStore;

public:
    explicit MessageQueue();
    explicit MessageQueue(uint32_t queue_id, PagedFile* data_file);

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
    void force_flush_last_page();
    // Methods for QueueStore to initialize id and name when it
    // create a new queue.
    void set_queue_id(uint32_t queue_id) { this->queue_id_ = queue_id; }
    void set_data_file(PagedFile* data_file) { this->data_file_ = data_file; }
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
    // Date file
    PagedFile* data_file_;
};

// forced 1
#define DATA_FILE_SPLITS 1

class QueueStore {
public:
    QueueStore(const String& location);
    ~QueueStore();

    void put(const String& queue_name, const MemBlock& message);
    Vector<MemBlock> get(const String& queue_name, long offset, long size);

protected:
    ConcurrentHashMap<String, SharedPtr<MessageQueue>> queues_{1000010};

    SharedPtr<MessageQueue> find_or_create_queue(const String& queue_name);
    SharedPtr<MessageQueue> find_queue(const String& queue_name) const;

    String data_file_path(int idx) const {
        return location_ + "/messages_" + std::to_string(idx) + ".data";
    }
    String index_file_path() const { return location_ + "/index.data"; }
    // load all queues' metadatas from disk file index.data
    void load_queues_metadatas();
    // flush all queues' metadatas to disk file index.data
    void flush_queues_metadatas();

    // sweep caches
    std::mutex wb_mutex_;
    bool write_buffer_cleared = false;
    void disable_all_write_buffers();

private:
    String location_;
    PagedFile* data_files_[DATA_FILE_SPLITS];
    SteppedValue<uint32_t> next_queue_id_{1};
};

}  // namespace race2018

#endif  // QUEUE_RACE_STORAGE_H
