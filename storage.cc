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
    if (summary.write_offset + sizeof(IndexEntry) <= FILE_PAGE_AVALIABLE_SIZE) {
        uint16_t slot_off = summary.write_offset;
        summary.write_offset += sizeof(IndexEntry);
        return slot_off;
    }
    return NEGATIVE_OFFSET;
}

// not thread safe
void Queue::allocate_next_index_slot(uint64_t& page_off, uint64_t& slot_off) {
    page_off = cur_index_page_idx_.load();
    auto it = summary_map_.find(page_off);
    if (it != summary_map_.end()) {
        slot_off = allocate_index_slot(it->second);
    } else {
        slot_off = NEGATIVE_OFFSET;
    }

    // current index slot is full, allocate next page
    if (page_off == NEGATIVE_OFFSET || slot_off == NEGATIVE_OFFSET) {
        bool allocated = false;
        page_off = allocate_new_page(index_file_, page_off, cur_index_page_idx_, allocated);
        // concurrent insert, this will not cause replacing, refer to ttb's doc
        IndexPageSummary index_page_summary;
        index_page_summary.page_offset = page_off;
        auto it_res = summary_map_.insert(std::make_pair(page_off, index_page_summary));
        it = it_res.first;
        slot_off = allocate_index_slot(it->second);
        if (allocated) {
            summaries_.push_back(&it->second);
            ++index_page_num_;
        }
    }
}

// not thread safe
void Queue::allocate_next_data_slot(uint64_t& page_off, uint64_t& slot_off, uint16_t size) {
    page_off = cur_data_page_idx_.load();
    slot_off = cur_data_slot_off_.fetch_add(size);

    // current data slot is full, allocate next page
    if (page_off == NEGATIVE_OFFSET || slot_off + size > FILE_PAGE_AVALIABLE_SIZE) {
        bool allocated = false;
        page_off = allocate_new_page(data_file_, page_off, cur_data_page_idx_, allocated);
        cur_data_slot_off_.store(size);
        slot_off = 0;
        if (allocated) ++data_page_num_;
    }
}

// put is sequential to support index verification
void Queue::put(const MemBlock& message) {
    // find current active index page and allocate index slot
    uint64_t idx_off, idx_slot_off;
    allocate_next_index_slot(idx_off, idx_slot_off);

    // compress message and set msg_ptr and msg_size
    /* char msg_buf[LZ4_COMPRESSBOUND(2048)];
     * size_t msg_size =
     *     LZ4_compress_default((const char*)(message.ptr), msg_buf, message.size, sizeof(msg_buf));
     * void* msg_ptr = (void*)msg_buf; */
    size_t msg_size = message.size;
    void* msg_ptr = message.ptr;

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
    ++message_num_;
}

PageSegment generate_page_segment_from(const IndexPageSummary* summary, size_t idx,
                                       uint64_t& msg_offset, uint64_t& msg_num) {
    assert(msg_num > 0);

    PageSegment segment;
    segment.page_offset = summary->page_offset;
    segment.start_slot_offset =
        (msg_offset - MAX_INDEX_ENTRIES_IN_PAGE * idx) * INDEX_ENTRY_SLOT_SIZE;
    uint64_t num_this_seg = std::min(
        uint64_t((summary->write_offset - segment.start_slot_offset) / INDEX_ENTRY_SLOT_SIZE),
        msg_num);
    segment.end_slot_offset = segment.start_slot_offset + num_this_seg * INDEX_ENTRY_SLOT_SIZE;

    msg_offset += num_this_seg;
    msg_num -= num_this_seg;
    return segment;
}

Vector<PageSegment> Queue::find_index_segments(uint64_t msg_offset, uint64_t& msg_num) const {
    size_t first_idx = msg_offset / MAX_INDEX_ENTRIES_IN_PAGE;
    if (first_idx >= summaries_.size()) return Vector<PageSegment>();
    size_t last_idx = (msg_offset + msg_num - 1) / MAX_INDEX_ENTRIES_IN_PAGE;

    Vector<PageSegment> segments;
    for (auto it = first_idx; it <= last_idx; ++it) {
        segments.push_back(generate_page_segment_from(summaries_[it], it, msg_offset, msg_num));
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
            block.ptr = (void*)(new uint8_t[entry.raw_length + 1]);
            block.size = entry.raw_length;

            // decompress message to block
            /* int decompressed_size =
             *     LZ4_decompress_safe(ptr, (char*)block.ptr, entry.length, entry.raw_length);
             * assert(decompressed_size > 0 && decompressed_size == block.size); */
            memcpy(block.ptr, ptr, block.size);
            ((char*)block.ptr)[block.size] = '\0';
            messages.push_back(std::move(block));
        });
    });

    return messages;
}

SharedPtr<Queue> QueueStore::find_or_create_queue(const String& queue_name) {
    auto it = queues_.find(queue_name);
    if (it == queues_.end()) {
        SharedPtr<Queue> queue_ptr = std::make_shared<Queue>(&index_file_, &data_file_);
        auto it_res = queues_.insert(std::make_pair(queue_name, std::move(queue_ptr)));
        it = it_res.first;
        if (it_res.second) {  // creates a new queue
            auto& q = it->second;
            q->set_queue_id(queue_id_generator_.next());
            q->set_queue_name(queue_name);

            DLOG("Created a new queue, id: %d, name: %s", q.get_queue_id(),
                 q.get_queue_name().c_str());
        }
    }
    return it->second;
}

SharedPtr<Queue> QueueStore::find_queue(const String& queue_name) const {
    auto it = queues_.find(queue_name);
    return it == queues_.end() ? nullptr : it->second;
}

void QueueStore::put(const String& queue_name, const MemBlock& message) {
    auto q_ptr = find_or_create_queue(queue_name);
    q_ptr->put(message);
    // free MemBlock
    free(message.ptr);
}

Vector<MemBlock> QueueStore::get(const String& queue_name, long offset, long size) {
    auto q_ptr = find_queue(queue_name);
    if (q_ptr) return q_ptr->get(offset, size);
    // return empty list when not found
    return Vector<MemBlock>();
}
}  // namespace race2018
