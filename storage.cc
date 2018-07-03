#include "storage.h"

#include <lz4.h>

namespace race2018 {

// not thread safe
uint16_t MessageQueue::allocate_index_slot(IndexPageSummary& summary) {
    if (summary.write_offset + sizeof(IndexEntry) <= FILE_PAGE_AVALIABLE_SIZE) {
        uint16_t slot_off = summary.write_offset;
        summary.write_offset += sizeof(IndexEntry);
        return slot_off;
    }
    return 65535;
}

// not thread safe
bool MessageQueue::allocate_next_index_slot(uint64_t& page_off, uint16_t& slot_off) {
    page_off = cur_index_page_idx_;
    if (page_off != NEGATIVE_OFFSET) {
        slot_off = allocate_index_slot(summaries_.back());
    }

    // current index slot is full, allocate next page
    if (page_off == NEGATIVE_OFFSET || slot_off == 65535) {
        cur_index_page_idx_ = page_off = index_file_->next_page_offset();
        // concurrent insert, this will not cause replacing, refer to ttb's doc
        IndexPageSummary index_page_summary;
        index_page_summary.page_offset = page_off;
        slot_off = allocate_index_slot(index_page_summary);
        summaries_.push_back(index_page_summary);
        ++index_page_num_;
        return true;
    }

    return false;
}

// not thread safe
bool MessageQueue::allocate_next_data_slot(uint64_t& page_off, uint16_t& slot_off, uint16_t size) {
    page_off = cur_data_page_idx_;
    if (cur_data_slot_off_ + size <= FILE_PAGE_AVALIABLE_SIZE) {
        slot_off = cur_data_slot_off_;
        cur_data_slot_off_ += size;
    } else {
        slot_off = 65535;
    }

    // current data slot is full, allocate next page
    if (page_off == NEGATIVE_OFFSET || slot_off == 65535) {
        cur_data_page_idx_ = page_off = data_file_->next_page_offset();
        cur_data_slot_off_ = size;
        slot_off = 0;
        ++data_page_num_;
        return true;
    }

    return false;
}

void MessageQueue::flush_all() const {
    if (index_entries_.empty() && messages_.empty()) return;

    std::unique_lock<std::mutex> lock(mutex_);
    while (!index_entries_.empty()) {
        flush_indices_of_page(index_entries_.front().page, false);
    }
    while (!messages_.empty()) {
        flush_messages_of_page(messages_.front().page, false);
    }
}

void MessageQueue::flush_messages_of_last_page(bool lock = true) const {
    if (messages_.empty()) return;
    flush_messages_of_page(messages_.front().page, lock);
}

void MessageQueue::flush_indices_of_last_page(bool lock = true) const {
    if (index_entries_.empty()) return;
    flush_indices_of_last_page(index_entries_.front().page, lock);
}

void MessageQueue::flush_indices_of_page(uint64_t page_offset, bool lock) const {
    if (index_entries_.empty() || index_entries_.front().page != page_offset) return;
    if (lock) mutex_.lock();
    index_file_->write_to_page(page_offset, [this, page_offset](FilePagePtr& ptr) {
        while (!index_entries_.empty() && index_entries_.front().page == page_offset) {
            auto& index = index_entries_.front();
            write_to_buf(ptr->content + index.offset, index.entry);
            index_entries_.pop();
        }
    });
    if (lock) mutex_.unlock();
}

void MessageQueue::flush_messages_of_page(uint64_t page_offset, bool lock) const {
    if (messages_.empty() || messages_.front().page != page_offset) return;
    if (lock) mutex_.lock();
    data_file_->write_to_page(page_offset, [this, page_offset](FilePagePtr& ptr) {
        while (!messages_.empty() && messages_.front().page == page_offset) {
            auto paged_msg = messages_.front();
            memcpy(ptr->content + paged_msg.offset, paged_msg.msg.ptr, paged_msg.msg.size);
            ::free(paged_msg.msg.ptr);
            messages_.pop();
        }
    });
    if (lock) mutex_.unlock();
}

// put is sequential to support index verification
void MessageQueue::put(const MemBlock& message) {
    // find current active index page and allocate index slot
    PagedIndex index;
    bool new_index_page = allocate_next_index_slot(index.page, index.offset);

    // find current active data page and allocate payload slot
    PagedMessage msg;
    msg.msg = message;
    bool new_data_page = allocate_next_data_slot(msg.page, msg.offset, message.size);
    if (new_data_page) flush_messages_of_last_page();
    messages_.push(msg);

    // commit log to index slot, current active index page offset
    // is @idx_off, current active index slot offset (in page) is
    // @idx_slot_off
    IndexEntry& index_entry = index.entry;
    index_entry.offset = msg.page + msg.offset;
    index_entry.raw_length = message.size;
    index_entry.length = message.size;
    if (new_index_page) flush_indices_of_last_page();
    index_entries_.push(index);

    // update index page summary
    auto& last_idx = const_cast<IndexPageSummary&>(summaries_.back());
    last_idx.size++;

    // update statistics
    ++message_num_;
}

PageSegment generate_page_segment_from(const IndexPageSummary& summary, size_t idx,
                                       uint64_t& msg_offset, uint64_t& msg_num) {
    assert(msg_num > 0);

    PageSegment segment;
    segment.page_offset = summary.page_offset;
    segment.start_slot_offset =
        (msg_offset - MAX_INDEX_ENTRIES_IN_PAGE * idx) * INDEX_ENTRY_SLOT_SIZE;
    uint64_t num_this_seg = std::min(
        uint64_t((summary.write_offset - segment.start_slot_offset) / INDEX_ENTRY_SLOT_SIZE),
        msg_num);
    segment.end_slot_offset = segment.start_slot_offset + num_this_seg * INDEX_ENTRY_SLOT_SIZE;

    msg_offset += num_this_seg;
    msg_num -= num_this_seg;
    return segment;
}

Vector<PageSegment> MessageQueue::find_index_segments(uint64_t msg_offset,
                                                      uint64_t& msg_num) const {
    size_t first_idx = msg_offset / MAX_INDEX_ENTRIES_IN_PAGE;
    if (first_idx >= summaries_.size()) return Vector<PageSegment>();
    size_t last_idx = (msg_offset + msg_num - 1) / MAX_INDEX_ENTRIES_IN_PAGE;

    Vector<PageSegment> segments;
    for (auto it = first_idx; it <= last_idx; ++it) {
        segments.push_back(generate_page_segment_from(summaries_[it], it, msg_offset, msg_num));
    }

    return segments;
}

Vector<MemBlock> MessageQueue::get(long offset, long number) const {
    flush_all();

    // find index pages
    uint64_t left = number;
    Vector<PageSegment> page_segs = find_index_segments(offset, left);
    if (page_segs.empty()) return Vector<MemBlock>();
    // get accurate message numbers
    number -= left;

    Vector<MemBlock> messages;
    messages.reserve(number);

    Queue<IndexEntry> entries;
    index_file_->read_and_handle<IndexEntry>(
        page_segs, [&entries](const IndexEntry& entry) { entries.push(entry); });

    while (!entries.empty()) {
        // for each data page, load messages
        uint64_t page_offset = FILE_PAGE_OFFSET_OF(entries.front().offset);
        uint16_t start_slot_offset = SLOT_OFFSET_OF(entries.front().offset);
        uint16_t size = 0;
        Queue<IndexEntry> entries_in_page;

        while (!entries.empty() && FILE_PAGE_OFFSET_OF(entries.front().offset) == page_offset) {
            auto& entry = entries.front();
            size = SLOT_OFFSET_OF(entry.offset) - start_slot_offset + entry.length;
            entries_in_page.push(entry);
            entries.pop();
        }

        data_file_->read_and_handle(
            page_offset, start_slot_offset, size, [&messages, &entries_in_page](FilePagePtr& ptr) {
                while (!entries_in_page.empty()) {
                    auto& entry = entries_in_page.front();

                    // load and copy message
                    MemBlock block;
                    block.ptr = (void*)(new uint8_t[entry.raw_length + 1]);
                    block.size = entry.raw_length;

                    memcpy(block.ptr, ptr->content + SLOT_OFFSET_OF(entry.offset), block.size);
                    ((char*)block.ptr)[block.size] = '\0';
                    messages.push_back(std::move(block));

                    entries_in_page.pop();
                }
            });
    }

    return messages;
}

SharedPtr<MessageQueue> QueueStore::find_or_create_queue(const String& queue_name) {
    auto it = queues_.find(queue_name);
    if (it == queues_.end()) {
        SharedPtr<MessageQueue> queue_ptr =
            std::make_shared<MessageQueue>(&index_file_, &data_file_);
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

SharedPtr<MessageQueue> QueueStore::find_queue(const String& queue_name) const {
    auto it = queues_.find(queue_name);
    return it == queues_.end() ? nullptr : it->second;
}

void QueueStore::put(const String& queue_name, const MemBlock& message) {
    auto q_ptr = find_or_create_queue(queue_name);
    q_ptr->put(message);
}

Vector<MemBlock> QueueStore::get(const String& queue_name, long offset, long size) {
    auto q_ptr = find_queue(queue_name);
    if (q_ptr) return q_ptr->get(offset, size);
    // return empty list when not found
    return Vector<MemBlock>();
}
}  // namespace race2018
