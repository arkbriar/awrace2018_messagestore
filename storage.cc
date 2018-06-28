#include "storage.h"

#include <lz4.h>

namespace race2018 {

uint64_t Queue::allocate_new_page(PagedFile* file, uint64_t prev_offset, Atomic<uint64_t>& offset,
                                  bool& allocated) {
    uint64_t off = prev_offset;
    if ((allocated = offset.compare_exchange_strong(off, PLACEHOLDER_OFFSET))) {
        off = file->next_page_offset();
        offset.store(off);
    } else {
        // spin wait util offset is not place holder
        while (off == PLACEHOLDER_OFFSET) {
            off = offset.load();
        }
    }
    return off;
}

uint64_t Queue::allocate_new_page(PagedFile* file, Atomic<uint64_t>& offset, bool& allocated) {
    return allocate_new_page(file, NEGATIVE_OFFSET, offset, allocated);
}

// not thread safe
uint64_t Queue::allocate_index_slot(IndexPageSummary& summary) {
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
        index_page_summary.page_offset = page_off;
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
    if (page_off == NEGATIVE_OFFSET || slot_off >= FILE_PAGE_AVALIABLE_SIZE) {
        bool allocated = false;
        page_off = allocate_new_page(data_file_, page_off, cur_data_page_idx_, allocated);
        cur_data_slot_off_.store(size);
        slot_off = size;
        if (allocated) data_page_num_.fetch_add(1);
    }
}

// put is sequential to support index verification
void Queue::put(const MemBlock& message) {
    // find current active index page and allocate index slot
    uint64_t idx_off, idx_slot_off;
    allocate_next_index_slot(idx_off, idx_slot_off);

    // compress message and set msg_ptr and msg_size
    char msg_buf[LZ4_COMPRESSBOUND(2048)];
    size_t msg_size =
        LZ4_compress_default((const char*)(message.ptr), msg_buf, message.size, sizeof(msg_buf));
    void* msg_ptr = (void*)msg_buf;

    // find current active data page and allocate payload slot
    uint64_t data_off, data_slot_off;
    allocate_next_data_slot(data_off, data_slot_off, msg_size);
    // write to data file
    data_file_->write_to_page(msg_ptr, msg_size, data_off, data_slot_off);

    // commit log to index slot, current active index page offset
    // is @idx_off, current active index slot offset (in page) is
    // @idx_slot_off
    IndexEntry index_entry;
    index_entry.offset = data_off + data_slot_off;
    index_entry.raw_length = message.size;
    index_entry.length = msg_size;
    index_file_->write_to_page(index_entry, idx_off, idx_slot_off);

    // update index page summary
    auto back_ptr = const_cast<IndexPageSummary*>(summaries_.back());
    back_ptr->size++;

    // update statistics
    message_num_.fetch_add(1);
}

PageSegment generate_page_segment_from(const IndexPageSummary* summary, uint64_t& msg_offset,
                                       uint64_t& msg_num) {
    assert(msg_num > 0);

    PageSegment segment;
    segment.page_offset = summary->page_offset;
    segment.start_slot_offset = (msg_offset - summary->prefix_sum) * INDEX_ENTRY_SLOT_SIZE;
    uint64_t num_this_seg = std::min(uint64_t(MAX_INDEX_ENTRIES_IN_PAGE), msg_num);
    segment.end_slot_offset = segment.start_slot_offset + num_this_seg * INDEX_ENTRY_SLOT_SIZE;

    msg_offset += num_this_seg;
    msg_num -= num_this_seg;
    return segment;
}

size_t binary_search(const ConcurrentVector<const IndexPageSummary*>& summaries, uint64_t target) {
    size_t cur_size = summaries.size();
    size_t first = 0, last = cur_size;
    while (first < last) {
        size_t middle = first + (last - first) / 2;
        if (summaries[middle]->prefix_sum < target)
            last = middle + 1;
        else
            last = middle;
    }
    if (first == cur_size) return NPOSLL;
    return first;
}

Vector<PageSegment> Queue::find_index_segments(uint64_t msg_offset, uint64_t& msg_num) const {
    size_t first_idx = binary_search(summaries_, msg_offset);
    if (first_idx == NPOSLL) return Vector<PageSegment>();
    size_t last_idx = binary_search(summaries_, msg_offset + msg_num - 1);

    Vector<PageSegment> segments;
    for (auto it = first_idx; it <= last_idx; ++it) {
        segments.push_back(generate_page_segment_from(summaries_[it], msg_offset, msg_num));
    }

    return segments;
}

Vector<MemBlock> Queue::get(long offset, long number) const {
    // find index pages
    uint64_t left = number;
    Vector<PageSegment> page_segs = find_index_segments(offset, left);
    if (page_segs.empty()) return Vector<MemBlock>();
    // get accurate message numbers
    number -= left;

    Vector<MemBlock> messages;
    messages.reserve(number);

    index_file_->read_and_handle<IndexEntry>(page_segs, [this, &messages](const IndexEntry& entry) {
        // for each index entry, read message
        data_file_->raw_read_and_handle(entry.offset, [&entry, &messages](const char* ptr) {
            MemBlock block;
            block.ptr = (void*)(new uint8_t[entry.raw_length]);
            block.size = entry.raw_length;

            // decompress message to block
            LZ4_decompress_safe(ptr, (char*)block.ptr, entry.length, entry.raw_length);
            messages.push_back(std::move(block));
        });
    });

    return messages;
}

void QueueStore::put(const String& queue_name, const MemBlock& message) {
    ConstConcurrentHashMapAccessor<String, SharedPtr<Queue>> it;
    SharedPtr<Queue> queue_ptr = std::make_shared<Queue>();
    if (queues_.insert(it, std::make_pair(queue_name, queue_ptr))) {
        // create a new queue
        auto& q = const_cast<SharedPtr<Queue>&>(it->second);
        q->set_queue_id(next_queue_id());
        q->set_queue_name(queue_name);
        q->set_files(&index_file_, &data_file_);

        DLOG("Created a new queue, id: %d, name: %s", q.get_queue_id(), q.get_queue_name().c_str());
    }

    // put into queue
    auto& q = const_cast<SharedPtr<Queue>&>(it->second);
    q->put(message);

    // free MemBlock
    free(message.ptr);
}

Vector<MemBlock> QueueStore::get(const String& queue_name, long offset, long number) {
    ConstConcurrentHashMapAccessor<String, SharedPtr<Queue>> it;
    if (queues_.find(it, queue_name)) {
        auto& q = it->second;
        return q->get(offset, number);
    }
    // return empty list when not found
    return Vector<MemBlock>();
}
}  // namespace race2018
