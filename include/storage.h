#ifndef QUEUE_RACE_STORAGE_H
#define QUEUE_RACE_STORAGE_H

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

struct __attribute__((__packed__)) IndexPageSummary {
    uint32_t queue_id;
    uint32_t prefix_sum = 0;
    volatile uint16_t size = 0;
    uint16_t write_offset = 0;
};

#define ONE_TERA_BYTES (1024ULL * 1024ULL * 1024ULL * 1024ULL)
// Maximum one tera bytes pages
#define PAGE_OFFSET_LIMIT (ONE_TERA_BYTES / FILE_PAGE_SIZE)

class PagedFile {
    template <class T>
    union Convertor {
        T t;
        uint8_t bytes[sizeof(T)];
    };

public:
    using LoaderFunc = FilePage (*)(uint64_t);
    // FIXME here loader is empty
    PagedFile(const String& file, size_t cache_capacity)
        : file_(file), cache_(nullptr, cache_capacity) {}

protected:
    void ensure_file_size(uint64_t size);

public:
    template <class T>
    T read_from_page(uint64_t page_offset, uint16_t slot_offset) {
        assert(page_offset < PAGE_OFFSET_LIMIT &&
               slot_offset + sizeof(T) < FILE_PAGE_AVALIABLE_SIZE);

        // FIXME load page from lru cache or disk
        FilePage* page = nullptr;
        Convertor<T> convertor;
        memcpy(convertor.bytes, page->content[slot_offset], sizeof(T));
        return convertor.t;
    }

    template <class T>
    void write_to_page(const T& data, uint64_t page_offset, uint16_t slot_offset) {
        assert(page_offset < PAGE_OFFSET_LIMIT &&
               slot_offset + sizeof(T) < FILE_PAGE_AVALIABLE_SIZE);

        // FIXME load page from lru cache or disk
        FilePage* page = nullptr;
        Convertor<T>* convertor = reinterpret_cast<Convertor<T>*>(page->content + slot_offset);
        convertor->t = data;
    }

    void write_to_page(const void* data, size_t size, uint64_t page_offset, uint16_t slot_offset) {
        assert(page_offset < PAGE_OFFSET_LIMIT && slot_offset + size < FILE_PAGE_AVALIABLE_SIZE);
        // FIXME load page from lru cache or disk
        FilePage* page = nullptr;
        memcpy(page->content + slot_offset, data, size);
    }

    uint64_t next_page_offset() { return next_page_offset_.fetch_add(FILE_PAGE_SIZE); }

private:
    int fd_;
    String file_;
    Atomic<uint64_t> next_page_offset_ = 0;
    ConcurrentLruCache<uint64_t, FilePage*> cache_;
};

#define NEGATIVE_OFFSET uint64_t(-1LL)
#define PLACEHOLDER_OFFSET uint64_t(-2LL)

class QueueStore;
class Queue {
public:
    explicit Queue() {}
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

    uint64_t binary_search_index_page(uint64_t offset);

private:
    uint32_t queue_id_;
    String queue_name_;

    // Statistics, they are not accurate at some timepoint,
    // so use them just as hints.
    Atomic<uint32_t> message_num_ = 0;
    Atomic<uint32_t> index_page_num_ = 0;
    Atomic<uint32_t> data_page_num_ = 0;

    ConcurrentHashMap<uint64_t, IndexPageSummary> summary_map_;
    ConcurrentVector<const IndexPageSummary*> summaries_;

    // Atomic cursor for writing data
    Atomic<uint64_t> cur_index_page_idx_ = NEGATIVE_OFFSET;
    Atomic<uint64_t> cur_data_page_idx_ = NEGATIVE_OFFSET;
    Atomic<uint64_t> cur_data_slot_off_ = 0;

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

    Atomic<uint32_t> next_queue_id_ = 0;
    ConcurrentHashMap<String, Queue> queues_;
};

}  // namespace race2018

#endif  // QUEUE_RACE_STORAGE_H
