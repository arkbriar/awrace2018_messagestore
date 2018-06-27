#include "storage.h"

namespace race2018 {

uint64_t Queue::allocate_new_page(PagedFile* file, uint64_t prev_offset, Atomic<uint64_t>& offset,
                                  bool& allocated) {
    allocated = false;
    uint64_t off = offset.load();
    if (off == prev_offset) {
        if (offset.compare_exchange_strong(off, PLACEHOLDER_OFFSET)) {
            allocated = true;
            off = file->next_page_offset();
            offset.store(off);
            return off;
        }
    }
    // busy waiting util offset is not place holder
    while (off == PLACEHOLDER_OFFSET) {
        off = offset.load();
    }

    return off;
}

uint64_t Queue::allocate_new_page(PagedFile* file, Atomic<uint64_t>& offset, bool& allocated) {
    return allocate_new_page(file, NEGATIVE_OFFSET, offset, allocated);
}

uint64_t Queue::allocate_index_slot(IndexPageSummary& summary) {
    assert(size < (1U << 16) - 1);
    if (summary.position + sizeof(IndexEntry) < FILE_PAGE_SIZE) {
        uint16_t slot_off = summary.position;
        summary.position += sizeof(IndexEntry);
        return slot_off;
    }
    return NEGATIVE_OFFSET;
}

// put is sequential to support index verification
void Queue::put(const MemBlock& message) {
    // find current active index page and allocate index slot
    uint64_t idx_off = last_index_page_offset_.load();
    uint64_t idx_slot_off;
    ConstConcurrentHashMapAccessor<uint64_t, IndexPageSummary> it;
    summaries_.find(it, idx_off);
    if (idx_off == NEGATIVE_OFFSET || it.empty() ||
        (idx_slot_off = allocate_index_slot(const_cast<IndexPageSummary&>(it->second))) ==
            NEGATIVE_OFFSET) {
        bool allocated = false;
        bool new_idx_off =
            allocate_new_page(index_file_, idx_off, last_index_page_offset_, allocated);
        summaries_.insert(it, std::make_pair(new_idx_off, IndexPageSummary{.queue_id = queue_id_}));
        if (allocated && idx_off == NEGATIVE_OFFSET) {
            first_index_page_offset_.store(new_idx_off);
        }
        idx_off = new_idx_off;
    }

    // commit log to index slot, current active index page offset
    // is @idx_off, current active index slot offset (in page) is
    // @idx_slot_off

    // compress message
    size_t msg_size = message.size;

    // find current active data page and allocate payload slot

    // write
}

Vector<MemBlock> Queue::get(long offset, long number) const {
    // TODO impl get

    return Vector<MemBlock>();
}

void QueueStore::put(const String& queue_name, const MemBlock& message) {
    ConstConcurrentHashMapAccessor<String, Queue> it;
    if (queues_.insert(it, queue_name)) {
        // create a new queue
        auto& q = const_cast<Queue&>(it->second);
        q.set_queue_id(next_queue_id());
        q.set_queue_name(queue_name);
        q.set_files(&index_file_, &data_file_);

        LOG("Created a new queue, id: %d, name: %s", q.get_queue_id(), q.get_queue_name().c_str());
    }

    // put into queue
    auto& q = const_cast<Queue&>(it->second);
    q.put(message);

    // free MemBlock
    free(message.ptr);
}

Vector<MemBlock> QueueStore::get(const String& queue_name, long offset, long number) {
    ConstConcurrentHashMapAccessor<String, Queue> it;
    if (queues_.find(it, queue_name)) {
        auto& q = it->second;
        return q.get(offset, number);
    }
    // return empty list when not found
    return Vector<MemBlock>();
}
}  // namespace race2018
