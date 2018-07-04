#ifndef QUEUE_RACE_STORAGE_H
#define QUEUE_RACE_STORAGE_H

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <cerrno>
#include <mutex>

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
    msg.size = (src[size - 1] == '\0') ? size - 1 : size;
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
#define GIGA_BYTES(n) ((uint64_t(n)) << 30)
#define TERA_BYTES(n) (uint64_t(n) << 40)
#define FILE_PAGE_SIZE KILO_BYTES(4)

// File page header
struct __attribute__((__packed__)) FilePageHeader {};

#define FILE_PAGE_HEADER_SIZE sizeof(FilePageHeader)
#define FILE_PAGE_AVALIABLE_SIZE (FILE_PAGE_SIZE - FILE_PAGE_HEADER_SIZE)

// File page
struct __attribute__((__packed__)) FilePage {
    FilePageHeader header;
    char content[FILE_PAGE_AVALIABLE_SIZE];
};
#define FILE_PAGE_SLOT_OFFSET_OF(offset) (offset & (FILE_PAGE_SIZE - 1))
#define FILE_PAGE_OFFSET_OF(offset) \
    (offset - FILE_PAGE_HEADER_SIZE - FILE_PAGE_SLOT_OFFSET_OF(offset))
#define FILE_PAGE_OF_PTR(ptr) ((FilePage*)ptr)
#define FILE_OFFSET_OF(page_offset, slot_offset) (page_offset + FILE_PAGE_HEADER_SIZE + slot_offset)

#define PAGE_OFFSET_LIMIT TERA_BYTES(1)

extern void* map_file_page(int fd, size_t offset, bool readonly);
extern void unmap_file_page(void* addr);
class MappedFilePagePtr {
public:
    MappedFilePagePtr(void* addr) : file_page_((FilePage*)addr) {}
    MappedFilePagePtr(int fd, size_t offset, bool readonly) {
        assert(offset == FILE_PAGE_OFFSET_OF(offset));
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

#define NEGATIVE_OFFSET uint64_t(-1LL)

using FilePageReader = std::function<void(const char*)>;
using FilePageWriter = std::function<void(char*)>;
class PagedFile {
public:
    PagedFile(const String& file);
    ~PagedFile();

    const String& get_file() const { return this->file_; }
    size_t size() const { return this->size_; }
    void read(uint64_t offset, const FilePageReader& reader);
    void read(uint64_t offset, FilePage* page);
    void write(uint64_t offset, const FilePageWriter& writer);
    void write(uint64_t offset, const FilePage* page);
    void mapped_read(uint64_t offset, const FilePageReader& reader);
    void mapped_write(uint64_t offset, const FilePageWriter& writer);
    uint64_t next_page_offset();

protected:
    SteppedValue<uint64_t> page_offset{FILE_PAGE_SIZE};

private:
    int fd_ = -1;
    size_t size_ = 0;
    String file_;
};

class QueueStore;
/* class MessageCache {
 * public:
 *     void push(uint64_t msg_offset, const MemBlock& msg) {
 *         // if not continuous, which means cache miss, drop all buffered messages.
 *         if (msgs_.empty() || msg_offset != msg_offset_ + 1) {
 *             Queue<MemBlock> empty;
 *             msgs_.swap(empty);
 *         }
 *         msgs_.push(msg);
 *         msg_offset_ = msg_offset;
 *     }
 *
 *     bool pop(MemBlock& msg) {
 *         if (msgs_.empty()) return false;
 *         msg = msgs_.front();
 *         msgs_.pop();
 *         ++msg_offset_;
 *         return true;
 *     }
 *
 *     size_t size() const { return msgs_.size(); }
 *
 *     uint64_t msg_offset_;
 *     Queue<MemBlock> msgs_;
 * }; */

struct __attribute__((__packed__)) MessagePageIndex {
    uint64_t page_offset;
    uint64_t prev_total_msg_size;
    uint16_t msg_size = 0;
    MessagePageIndex() {}
    MessagePageIndex(uint64_t page_offset, uint64_t prev_total_msg_size)
        : page_offset(page_offset), prev_total_msg_size(prev_total_msg_size) {}
    uint64_t total_msg_size() const { return prev_total_msg_size + msg_size; }
};

class MessageQueue {
    friend class QueueStore;

public:
    explicit MessageQueue(PagedFile* data_file);

    uint32_t get_queue_id() const { return this->queue_id_; }
    const String& get_queue_name() const { return this->queue_name_; }

    void put(const MemBlock& message);
    Vector<MemBlock> get(uint64_t offset, uint64_t number);

protected:
    void read_msgs(const MessagePageIndex& index, uint64_t& offset, uint64_t& num, const char* ptr,
                   Vector<MemBlock>& msgs);
    size_t binary_search_indices(uint64_t msg_offset) const;
    // Methods for message slots in data page
    uint64_t next_message_slot(uint64_t& page_offset, uint16_t& slot_offset, uint16_t size);
    // Methods for write/flushing data
    void write_to_last_page(const MemBlock& msg, uint16_t slot_offset);
    void flush_last_page(uint64_t page_offset, bool release);
    void flush_last_page() { flush_last_page(cur_data_page_off_, true); }

    // Methods for QueueStore to initialize id and name when it
    // create a new queue.
    void set_queue_id(uint32_t queue_id) { this->queue_id_ = queue_id; }
    void set_queue_name(const String& queue_name) { this->queue_name_ = queue_name; }

    struct __attribute__((__packed__)) MessageQueueIndexHeader {
        uint32_t queue_id;
        uint32_t name_size;
        uint64_t page_offset;
        uint16_t slot_offset;
        uint32_t indices_size;

        uint64_t index_size() const { return sizeof(MessageQueueIndexHeader) + extra_length(); }
        uint64_t extra_length() const {
            return name_size + indices_size * sizeof(MessagePageIndex);
        }
    };

    void construct_header(MessageQueueIndexHeader& hdr) {
        hdr.queue_id = queue_id_;
        hdr.name_size = queue_name_.size();
        hdr.page_offset = cur_data_page_off_;
        hdr.slot_offset = cur_data_slot_off_;
        hdr.indices_size = paged_message_indices_.size();
    }

    void flush_queue_metadata(int fd) {
        MessageQueueIndexHeader header;
        construct_header(header);
        size_t buf_len = header.index_size();

        char* buf[buf_len];
        buffer::write_to_buf(buf, header);
        memcpy(buf + sizeof(MessageQueueIndexHeader), queue_name_.c_str(), header.name_size);
        auto ptr = buf + sizeof(MessageQueueIndexHeader) + header.name_size;
        for (auto& index : paged_message_indices_) {
            buffer::write_to_buf(ptr, index);
            ptr += sizeof(index);
        }
        ::write(fd, buf, buf_len);
    }

    void load_queue_metadata(const MessageQueueIndexHeader& hdr, const char* buf) {
        queue_id_ = hdr.queue_id;
        cur_data_page_off_ = hdr.page_offset;
        cur_data_slot_off_ = hdr.slot_offset;
        queue_name_ = String(buf, hdr.name_size);
        paged_message_indices_.reserve(hdr.indices_size);

        MessagePageIndex msg_index;
        auto ptr = buf + hdr.name_size;
        for (uint32_t i = 0; i < hdr.indices_size; ++i) {
            buffer::read_from_buf(ptr, msg_index);
            paged_message_indices_.push_back(msg_index);
            ptr += sizeof(MessagePageIndex);
        }
    }

private:
    uint32_t queue_id_ = -1;
    String queue_name_;

    // Cursors for writing data
    uint64_t cur_data_page_off_ = NEGATIVE_OFFSET;
    uint16_t cur_data_slot_off_ = 0;

    // Read cache
    /* MessageCache read_cache_; */
    // Write buffer
    std::mutex wq_mutex_;
    FilePage* last_page_;
    // Paged message index
    Vector<MessagePageIndex> paged_message_indices_;
    // Date file
    PagedFile* data_file_;
};

class QueueStore {
public:
    QueueStore(const String& location);
    ~QueueStore();

    void put(const String& queue_name, const MemBlock& message);
    Vector<MemBlock> get(const String& queue_name, long offset, long size);

protected:
    ConcurrentHashMap<String, SharedPtr<MessageQueue>> queues_;

    SharedPtr<MessageQueue> find_or_create_queue(const String& queue_name);
    SharedPtr<MessageQueue> find_queue(const String& queue_name) const;

    String data_file_path() const { return location_ + "/messages.data"; }
    String index_file_path() const { return location_ + "/index.data"; }
    // load all queues' metadatas from disk file index.data
    void load_queues_metadatas();
    // flush all queues' metadatas to disk file index.data
    void flush_queues_metadatas();

private:
    String location_;
    PagedFile data_file_;
    SteppedValue<uint32_t> next_queue_id_{1};
};

}  // namespace race2018

#endif  // QUEUE_RACE_STORAGE_H
