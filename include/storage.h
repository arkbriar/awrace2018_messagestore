#ifndef QUEUE_RACE_STORAGE_H
#define QUEUE_RACE_STORAGE_H

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <cerrno>

#include "common.h"

namespace race2018 {
struct MemBlock {
    /**
     * Pointer to the data. ptr is allocated by new char[LENGTH]
     */
    void* ptr;

    /**
     * Length of the data in bytes
     */
    size_t size;
};

#define FILE_PAGE_SIZE (16 * 1024)  // 16 kilo bytes
#define FILE_PAGE_OFFSET_OF(offset) (offset - (offset & (FILE_PAGE_SIZE - 1)))

struct __attribute__((__packed__)) FilePageHeader {
    uint32_t queue_id;
    // prev, next page offset
    uint64_t prev, next;
};

#define FILE_PAGE_HEADER_SIZE sizeof(FilePageHeader)
#define FILE_PAGE_AVALIABLE_SIZE (FILE_PAGE_SIZE - FILE_PAGE_HEADER_SIZE)

struct __attribute__((__packed__)) FilePage {
    FilePageHeader header;
    uint8_t content[FILE_PAGE_AVALIABLE_SIZE];
};

struct __attribute__((__packed__)) IndexEntry {
    uint64_t offset;
    uint16_t raw_length;
    uint16_t length;
};

#define INDEX_ENTRY_SLOT_SIZE sizeof(IndexEntry)
#define MAX_INDEX_ENTRIES_IN_PAGE (FILE_PAGE_AVALIABLE_SIZE / INDEX_ENTRY_SLOT_SIZE)

struct __attribute__((__packed__)) IndexPageSummary {
    uint64_t page_offset;
    volatile uint16_t size = 0;
    uint16_t write_offset = 0;
};

#define ONE_TERA_BYTES (1024ULL * 1024ULL * 1024ULL * 1024ULL * 1024ULL)
// Maximum one tera bytes pages
#define PAGE_OFFSET_LIMIT ONE_TERA_BYTES

template <class T>
union ReadWriter {
    T t;
    uint8_t bytes[sizeof(T)];
};

// caller must ensure that src has enough bytes to read (>= sizeof(T)),
// otherwise behaviour is undefined
template <class T>
T read_from_buf(const void* src) {
    ReadWriter<T>* reader = reinterpret_cast<ReadWriter<T>*>(src);
    return reader->t;
}

// caller must ensure that src has enough bytes to write (>= sizeof(T)),
// otherwise behaviour is undefined
template <class T>
void write_to_buf(void* dest, const T& data) {
    ReadWriter<T>* writer = reinterpret_cast<ReadWriter<T>*>(dest);
    writer->t = data;
}

template <class T>
using RefHandleFunc = std::function<void(const T&)>;
template <class T>
using PtrHandleFunc = std::function<void(const T*)>;

template <class T>
void single_read_and_handle(const void* src, const RefHandleFunc<T>& handle_func) {
    ReadWriter<T>* reader;
    reader = reinterpret_cast<ReadWriter<T>*>(src);
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

class FilePagePtr {
public:
    FilePagePtr() : FilePagePtr(nullptr) {}
    FilePagePtr(FilePage* addr) : file_page_(addr) {}
    ~FilePagePtr() {
        if (file_page_ != nullptr) munmap(file_page_, FILE_PAGE_SIZE);
    }

    FilePage& operator*() const noexcept { return *file_page_; }
    FilePage* operator->() const noexcept { return file_page_; }
    explicit operator bool() const noexcept { return file_page_ != nullptr; }
    FilePage* get() const noexcept { return file_page_; }
    FilePagePtr& operator=(const FilePagePtr&) = delete;

private:
    FilePage* file_page_;
};

struct PageSegment {
    uint64_t page_offset;
    uint16_t start_slot_offset;
    uint16_t end_slot_offset;  // exclusive
};

static size_t read_file_size(int fd) {
    struct stat s;
    ::fstat(fd, &s);
    return s.st_size;
}

class PagedFile {
public:
    PagedFile(const String& file, size_t cache_capacity)
        : file_(file),
          cache_(
              [this](uint64_t offset) {
                  return std::make_shared<FilePagePtr>(this->allocate_new_page(offset));
              },
              cache_capacity) {
        fd_ = ::open(file.c_str(), O_RDWR | O_CREAT, 0644);
        assert(fd_ > 0);
        file_size_ = read_file_size(fd_);
    }
    ~PagedFile() { ::close(fd_); }

protected:
    bool ensure_file_size(uint64_t size) { return ::ftruncate(fd_, size) == 0; }

    FilePage* allocate_new_page(uint64_t offset) {
        void* address =
            ::mmap(nullptr, FILE_PAGE_SIZE, PROT_READ | PROT_WRITE, MAP_SHARED, fd_, offset);
        if (address == MAP_FAILED) {
            LOG("mmap failed due to errno: ", errno);
            return nullptr;
        }

        return reinterpret_cast<FilePage*>(address);
    }

public:
    void raw_read_and_handle(uint64_t page_offset, uint16_t slot_offset,
                             const PtrHandleFunc<char>& handle_func) const {
        page_offset = FILE_PAGE_OFFSET_OF(page_offset);
        assert(page_offset < PAGE_OFFSET_LIMIT);
        auto cache_handle = cache_[page_offset];
        FilePagePtr& page = *cache_handle.value();
        handle_func((const char*)(page->content + slot_offset));
    }

    void raw_read_and_handle(uint64_t offset, const PtrHandleFunc<char>& handle_func) const {
        uint16_t slot_offset = offset & (FILE_PAGE_SIZE - 1);
        offset -= slot_offset;
        this->raw_read_and_handle(offset, slot_offset, handle_func);
    }

    template <class T>
    void read_and_handle(uint64_t offset, const RefHandleFunc<T>& handle_func) const {
        uint16_t slot_offset = offset & (FILE_PAGE_SIZE - 1);
        offset -= slot_offset;
        this->read_and_handle<T>(offset, slot_offset, handle_func);
    }

    template <class T>
    void read_and_handle(uint64_t page_offset, uint16_t slot_offset,
                         const RefHandleFunc<T>& handle_func) const {
        page_offset = FILE_PAGE_OFFSET_OF(page_offset);
        assert(page_offset < PAGE_OFFSET_LIMIT &&
               slot_offset + sizeof(T) <= FILE_PAGE_AVALIABLE_SIZE);
        auto cache_handle = cache_[page_offset];
        FilePagePtr& page = *cache_handle.value();
        single_read_and_handle<T>((const void*)(page->content + slot_offset), handle_func);
    }

    template <class T>
    void read_and_handle(const Vector<PageSegment>& segments,
                         const RefHandleFunc<T>& handle_func) const {
        for (auto& seg : segments) {
            if (seg.end_slot_offset <= seg.start_slot_offset) continue;
            uint64_t page_offset = FILE_PAGE_OFFSET_OF(seg.page_offset);
            assert(page_offset < PAGE_OFFSET_LIMIT &&
                   seg.start_slot_offset + sizeof(T) <= FILE_PAGE_AVALIABLE_SIZE);
            size_t obj_size = (seg.end_slot_offset - seg.start_slot_offset) / sizeof(T);
            if (obj_size == 0) continue;
            auto cache_handle = cache_[page_offset];
            FilePagePtr& page = *cache_handle.value();
            batch_read_and_handle<T>((const void*)(page->content + seg.start_slot_offset), obj_size,
                                     handle_func);
        }
    }

    template <class T>
    void write_to_page(const T& data, uint64_t page_offset, uint16_t slot_offset) {
        page_offset = FILE_PAGE_OFFSET_OF(page_offset);

        assert(page_offset < PAGE_OFFSET_LIMIT &&
               slot_offset + sizeof(T) <= FILE_PAGE_AVALIABLE_SIZE);

        auto cache_handle = cache_[page_offset];
        FilePagePtr& page = *cache_handle.value();
        write_to_buf(page->content + slot_offset, data);
    }

    void write_to_page(const void* data, size_t size, uint64_t page_offset, uint16_t slot_offset) {
        page_offset = FILE_PAGE_OFFSET_OF(page_offset);

        assert(page_offset < PAGE_OFFSET_LIMIT && slot_offset + size <= FILE_PAGE_AVALIABLE_SIZE);
        auto cache_handle = cache_[page_offset];
        FilePagePtr& page = *cache_handle.value();
        memcpy(page->content + slot_offset, data, size);
    }

    uint64_t next_page_offset() {
        uint64_t offset = next_page_offset_.fetch_add(FILE_PAGE_SIZE);
        if (offset >= file_size_) {
            ensure_file_size(file_size_ = offset + 10000 * FILE_PAGE_SIZE);
        }
        return offset;
    }

private:
    int fd_;
    size_t file_size_;
    String file_;
    Atomic<uint64_t> next_page_offset_{0};
    mutable ConcurrentLruCache<uint64_t, SharedPtr<FilePagePtr>> cache_;
};

#define NPOSLL uint64_t(-1LL)
#define NEGATIVE_OFFSET NPOSLL
#define PLACEHOLDER_OFFSET uint64_t(-2LL)

class QueueStore;
class Queue {
public:
    Queue() {}
    explicit Queue(uint32_t queue_id, const String& queue_name)
        : queue_id_(queue_id), queue_name_(queue_name) {}

    uint32_t get_queue_id() const { return this->queue_id_; }
    const String& get_queue_name() const { return this->queue_name_; }
    void set_queue_id(uint32_t queue_id) { this->queue_id_ = queue_id; }
    void set_queue_name(const String& queue_name) { this->queue_name_ = queue_name; }
    void set_files(PagedFile* index_file, PagedFile* data_file) {
        this->index_file_ = index_file;
        this->data_file_ = data_file;
    }

    void put(const MemBlock& message);
    Vector<MemBlock> get(long offset, long number) const;

protected:
    static uint64_t allocate_new_page(PagedFile* file, Atomic<uint64_t>& offset, bool& allocated);
    static uint64_t allocate_new_page(PagedFile* file, uint64_t prev_offset,
                                      Atomic<uint64_t>& offset, bool& allocated);

    // Allocating next slots for writing. Currently they are not
    // thread-safe.
    static uint64_t allocate_index_slot(IndexPageSummary& summary);
    void allocate_next_index_slot(uint64_t& page_off, uint64_t& slot_off);
    void allocate_next_data_slot(uint64_t& page_off, uint64_t& slot_off, uint16_t size);

    Vector<PageSegment> find_index_segments(uint64_t msg_offset, uint64_t& msg_num) const;

private:
    uint32_t queue_id_;
    String queue_name_;

    // Statistics, they are not accurate at some timepoint,
    // so use them just as hints.
    uint64_t message_num_ = 0;
    uint64_t index_page_num_ = 0;
    uint64_t data_page_num_ = 0;

    // Summaries for reader and writer, as there are at most
    // 1 writer, thread safety is guaranteed by atomic cursors below
    ConcurrentHashMap<uint64_t, IndexPageSummary> summary_map_;
    ConcurrentVector<const IndexPageSummary*> summaries_;

    // Atomic cursors for writing data
    Atomic<uint64_t> cur_index_page_idx_{NEGATIVE_OFFSET};
    Atomic<uint64_t> cur_data_page_idx_{NEGATIVE_OFFSET};
    Atomic<uint64_t> cur_data_slot_off_{0};

    PagedFile *index_file_, *data_file_;
};

class QueueStore {
public:
    QueueStore(const String& location)
        : location_(location),
          index_file_(location + "/" + "index.data", 3000),
          data_file_(location + "/" + "messages.data", 262144) {}
    ~QueueStore() {}

    void put(const String& queue_name, const MemBlock& message);

    Vector<MemBlock> get(const String& queue_name, long offset, long number);

protected:
    uint32_t next_queue_id() { return next_queue_id_.fetch_add(1); }

private:
    String location_;
    PagedFile index_file_, data_file_;
    Atomic<uint32_t> next_queue_id_{0};
    ConcurrentHashMap<String, SharedPtr<Queue>> queues_;
};

}  // namespace race2018

#endif  // QUEUE_RACE_STORAGE_H
