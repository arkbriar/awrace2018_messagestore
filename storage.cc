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

// not thread safe
uint64_t Queue::allocate_index_slot(IndexPageSummary& summary) {
    assert(size < (1U << 16) - 1);
    if (summary.write_offset + sizeof(IndexEntry) < FILE_PAGE_SIZE) {
        uint16_t slot_off = summary.write_offset;
        summary.write_offset += sizeof(IndexEntry);
        return slot_off;
    }
    return NEGATIVE_OFFSET;
}

// not thread safe
void Queue::allocate_next_index_slot(uint64_t& page_off, uint64_t& slot_off) {
    page_off = cur_index_page_idx_.load();
    ConstConcurrentHashMapAccessor<uint64_t, IndexPageSummary> it;
    summary_map_.find(it, page_off);
    if (!it.empty()) slot_off = allocate_index_slot(const_cast<IndexPageSummary&>(it->second));

    // current index slot is full, allocate next page
    if (page_off == NEGATIVE_OFFSET || it.empty() || slot_off == NEGATIVE_OFFSET) {
        bool allocated = false;
        page_off = allocate_new_page(index_file_, page_off, cur_index_page_idx_, allocated);
        // concurrent insert, this will not cause replacing, refer to ttb's doc
        IndexPageSummary index_page_summary;
        index_page_summary.queue_id = queue_id_;
        if (!summaries_.empty()) {
            auto& back_ptr = summaries_.back();
            index_page_summary.prefix_sum = back_ptr->prefix_sum + back_ptr->size;
        }
        summary_map_.insert(it, std::make_pair(page_off, index_page_summary));
        slot_off = allocate_index_slot(const_cast<IndexPageSummary&>(it->second));
        if (allocated) {
            summaries_.push_back(&it->second);
            index_page_num_.fetch_add(1);
        }
    }
}

// not thread safe
void Queue::allocate_next_data_slot(uint64_t& page_off, uint64_t& slot_off, uint16_t size) {
    page_off = cur_data_page_idx_.load();
    slot_off = cur_data_slot_off_.load() + size;

    // current data slot is full, allocate next page
    if (page_off == NEGATIVE_OFFSET || >= FILE_PAGE_AVALIABLE_SIZE) {
        bool allocated = false;
        page_off = allocate_new_page(data_file_, page_off, cur_data_page_idx_, allocated);
        cur_data_slot_off_.store(size);
        slot_off = size;
        if (allocated) data_page_num_.fetch_add(1);
    }
}

uint64_t Queue::binary_search_index_page(uint64_t offset) {
    // TODO implement this binary search
    return -1LL;
}

// put is sequential to support index verification
void Queue::put(const MemBlock& message) {
    // find current active index page and allocate index slot
    uint64_t idx_off, idx_slot_off;
    allocate_next_index_slot(idx_off, idx_slot_off);

    // TODO compress message
    void* msg_ptr = message.ptr;
    size_t msg_size = message.size;

    // find current active data page and allocate payload slot
    uint64_t data_off, data_slot_off;
    allocate_next_data_slot(data_off, data_slot_off);
    // write to data file
    data_file_->write_to_page(msg_ptr, msg_size, data_off, data_slot_off);

    // commit log to index slot, current active index page offset
    // is @idx_off, current active index slot offset (in page) is
    // @idx_slot_off
    IndexEntry index_entry;
    index_entry.offset = data_off + data_slot_off + FILEPAGE_HEADER;
    index_entry.raw_length = message.size;
    index_entry.length = msg_size;
    index_file_->write_to_page(index_entry, idx_off, idx_slot_off);

    // update index page summary
    auto back_ptr = const_cast<IndexPageSummary*>(summaries_.back());
    back_ptr->size++;

    // update statistics
    message_num_.fetch_add(1);
}

Vector<MemBlock> Queue::get(long offset, long number) const {
    // binary search on index page summary
    uint64_t start_idx_off = binary_search_index_page(offset);
    uint64_t last_idx_off = binary_search_index_page(offset + number);
    if (start_idx_off == NEGATIVE_OFFSET) return Vector<MemBlock>();

    // TODO load all index entries
    Vector<IndexEntry> index_entries;
    uint64_t stored_number = 0;
    for (auto& entry : index_entries) {
        stored_number += entry.size;
    }
    number = std::min(number, long(stored_number));
    Vector<MemBlock> messages;
    messages.reserve(number);

    // foreach index entry, load data
    for (auto& entry : index_entries) {
        // read from data page
        MemBlock block;
        block.ptr = (void*)(new uint8_t[entry.raw_length]);
        block.size = entry.raw_length;

        // TODO decompress message to block

        messages.push_back(std::move(block));
    }
    return messages;
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
