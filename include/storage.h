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

struct __attribute__((__packed__)) FilePage {
    FilePageHeader header;
    uint8_t content[FILE_PAGE_SIZE - sizeof(header)];
};

struct __attribute__((__packed__)) IndexEntry {
    uint64_t offset;
    uint16_t length;
};

struct __attribute__((__packed__)) IndexPageSummary {
    uint32_t queue_id;
    uint32_t prefix_sum = 0;
    uint16_t size = 0;
    uint16_t position = 0;
};

class PagedFile {
public:
    using LoaderFunc = FilePage (*)(uint64_t);
    // FIXME here loader is empty
    PagedFile(const String& file, size_t cache_capacity)
        : file_(file), cache_(nullptr, cache_capacity) {}

    uint64_t next_page_offset() { return next_page_offset_.fetch_add(FILE_PAGE_SIZE); }

protected:
    FilePage new_page(uint64_t offset);

private:
    int fd_;
    String file_;
    Atomic<uint64_t> next_page_offset_ = 0;
    ConcurrentLruCache<uint64_t, FilePage> cache_;
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

    static uint64_t allocate_index_slot(IndexPageSummary& summary);

private:
    uint32_t queue_id_;
    Atomic<uint64_t> first_index_page_offset_ = NEGATIVE_OFFSET,
                     last_index_page_offset_ = NEGATIVE_OFFSET;
    Atomic<uint64_t> first_data_page_offset_ = NEGATIVE_OFFSET,
                     last_data_page_offset_ = NEGATIVE_OFFSET;
    Atomic<uint32_t> message_num_ = 0;
    Atomic<uint32_t> index_page_num_ = 0;
    Atomic<uint32_t> data_page_num_ = 0;
    String queue_name_;
    ConcurrentHashMap<uint64_t, IndexPageSummary> summaries_;

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
