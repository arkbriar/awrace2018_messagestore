#include "storage.h"

#include <lz4.h>

namespace race2018 {

void* map_file_page(int fd, size_t offset, bool readonly) {
    void* addr;
    if (readonly) {
        addr = ::mmap(nullptr, FILE_PAGE_SIZE, PROT_READ, MAP_SHARED, fd, offset);
    } else {
        addr = ::mmap(nullptr, FILE_PAGE_SIZE, PROT_READ | PROT_READ, MAP_SHARED, fd, offset);
    }
    if (addr == MAP_FAILED) {
        perror("mmap file failed");
        return nullptr;
    }
    return addr;
}

void unmap_file_page(void* addr) {
    if (addr == nullptr) return;
    ::munmap(addr, FILE_PAGE_SIZE);
}

PagedFile::PagedFile(const String& file) : file_(file) {
    fd_ = ::open(file.c_str(), O_RDWR | O_CREAT, 0644);
    assert(fd_ > 0);
    // read file size
    struct stat s;
    ::fstat(fd_, &s);
    size_ = s.st_size;
}

PagedFile::~PagedFile() {
    if (fd_ > 0) ::close(fd_);
}

void PagedFile::read(uint64_t offset, const FilePageReader& reader) {
    FilePage page;
    ssize_t ret = ::pread(fd_, (void*)&page, FILE_PAGE_SIZE, offset);
    if (ret == -1) {
        perror("read page failed");
        return;
    }
    reader(page.content);
}

void PagedFile::write(uint64_t offset, const FilePageWriter& writer) {
    FilePage page;
    writer(page.content);
    ssize_t ret = ::pwrite(fd_, (const void*)&page, FILE_PAGE_SIZE, offset);
    if (ret == -1) perror("write page failed");
}

void PagedFile::read(uint64_t offset, FilePage* page) {
    if (!page) return;
    ssize_t ret = ::pread(fd_, (void*)page, FILE_PAGE_SIZE, offset);
    if (ret == -1) perror("read page failed");
}

void PagedFile::write(uint64_t offset, const FilePage* page) {
    if (!page) return;
    ssize_t ret = ::pwrite(fd_, (const void*)page, FILE_PAGE_SIZE, offset);
    if (ret == -1) perror("write page failed");
}

void PagedFile::mapped_read(uint64_t offset, const FilePageReader& reader) {
    MappedFilePagePtr page_ptr(fd_, offset, true);
    if (!page_ptr) {
        LOG("fallback to pread");
        read(offset, reader);
    } else {
        reader(page_ptr->content);
    }
}

void PagedFile::mapped_write(uint64_t offset, const FilePageWriter& writer) {
    MappedFilePagePtr page_ptr(fd_, offset, false);
    if (!page_ptr) {
        LOG("fallback to pwrite");
        write(offset, writer);
    } else {
        writer(page_ptr->content);
    }
}

uint64_t PagedFile::next_page_offset() { return page_offset.next(); }

MessageQueue::MessageQueue() {}

MessageQueue::MessageQueue(uint32_t queue_id, const String& queue_name, PagedFile* data_file)
    : queue_id_(queue_id), queue_name_(queue_name), data_file_(data_file) {}

uint64_t MessageQueue::next_message_slot(uint64_t& page_offset, uint16_t& slot_offset,
                                         uint16_t size) {
    // first page will hold at most (queue_id / DATA_FILE_SPLITS) % 64 + 1 messages, this make write
    // more average
    if (paged_message_indices_.size() == 1 &&
        paged_message_indices_.back().msg_size >= ((queue_id_ / DATA_FILE_SPLITS) & 63) + 1) {
        uint64_t prev_data_page_off = cur_data_page_off_;
        page_offset = cur_data_page_off_ = data_file_->next_page_offset();
        slot_offset = 0;
        cur_data_slot_off_ = size;
        return prev_data_page_off;
    }

    // deal with the following pages
    if (cur_data_page_off_ == NEGATIVE_OFFSET ||
        cur_data_slot_off_ + size > FILE_PAGE_AVALIABLE_SIZE) {
        uint64_t prev_data_page_off = cur_data_page_off_;
        page_offset = cur_data_page_off_ = data_file_->next_page_offset();
        slot_offset = 0;
        cur_data_slot_off_ = size;
        return prev_data_page_off;
    } else {
        page_offset = cur_data_page_off_;
        slot_offset = cur_data_slot_off_;
        cur_data_slot_off_ += size;
        return cur_data_page_off_;
    }
}

void MessageQueue::flush_last_page(uint64_t page_offset, bool release) {
    if (!last_page_) return;

    std::unique_lock<std::mutex> lock(wq_mutex_);
    // write all messages in this queue to page of page_offset
    data_file_->write(page_offset, last_page_);
    if (release) {
        delete last_page_;
        last_page_ = nullptr;
    }
}

void MessageQueue::write_to_last_page(const MemBlock& msg, uint16_t slot_offset) {
    if (!last_page_) {
        // lazy allocate page
        last_page_ = new FilePage();
    }
    if (msg.size < 0x80) {
        last_page_->content[slot_offset] = msg.size;
        ::memcpy(last_page_->content + slot_offset + 1, msg.ptr, msg.size);
    } else {
        last_page_->content[slot_offset] = ((msg.size >> 8) | 0x80);
        last_page_->content[slot_offset + 1] = msg.size & 0xff;
        ::memcpy(last_page_->content + slot_offset + 2, msg.ptr, msg.size);
    }
}

// put is sequential to support index verification
void MessageQueue::put(const MemBlock& message) {
    // maximum support message size of
    assert(message.size <= 4000);

    uint16_t msg_size = message.size + 1;
    // use 2 bytes to record message length >= 128,
    // and use 1 byte to record message length < 128
    if (message.size >= 0x80) msg_size += 1;

    uint64_t page_offset;
    uint16_t slot_offset;
    uint64_t prev_page_offset = next_message_slot(page_offset, slot_offset, msg_size);
    if (prev_page_offset != page_offset) {
        // a new data page is allocated
        // flush messages of previous page
        flush_last_page(prev_page_offset, false);

        // create a new paged message index
        uint64_t prev_total_msg_size = 0;
        if (!paged_message_indices_.empty()) {
            auto& prev_index = paged_message_indices_.back();
            prev_total_msg_size = prev_index.total_msg_size();
        }
        paged_message_indices_.emplace_back(page_offset, prev_total_msg_size);
    }

    // write message into last page
    write_to_last_page(message, slot_offset);

    // free message
    ::free(message.ptr);

    ++paged_message_indices_.back().msg_size;
}

size_t MessageQueue::binary_search_indices(uint64_t msg_offset) const {
    size_t first = 0, last = paged_message_indices_.size();
    while (first < last) {
        size_t middle = first + (last - first) / 2;
        if (paged_message_indices_[middle].total_msg_size() <= msg_offset) {
            first = middle + 1;
        } else {
            last = middle;
        }
    }
    return first;
}

uint16_t MessageQueue::extract_message_length(const char*& ptr) {
    uint16_t msg_size = *ptr;
    if (msg_size < 0x80) {
        ++ptr;
        return msg_size;
    } else {
        ptr += 2;
        return ((msg_size & 0x7f) << 8) + (uint8_t)*ptr;
    }
}

void MessageQueue::read_msgs(const MessagePageIndex& index, uint64_t& offset, uint64_t& num,
                             const char* ptr, Vector<MemBlock>& msgs) {
    uint64_t cur_offset = index.prev_total_msg_size;
    uint16_t size = index.msg_size;
    if (offset >= cur_offset + size) return;

    auto begin = ptr;

    // skip to match offset
    while (cur_offset != offset && size > 0) {
        uint16_t msg_size = extract_message_length(begin);
        begin += msg_size;
        ++cur_offset, --size;
    }

    // read related messages to msgs
    while (num > 0 && size > 0) {
        uint16_t msg_size = extract_message_length(begin);
        msgs.push_back(new_memblock(begin, msg_size));
        begin += msg_size;
        ++cur_offset, ++offset, --num, --size;
    }
}

Vector<MemBlock> MessageQueue::get(uint64_t offset, uint64_t number) {
    // flush and release the last writing page
    if (last_page_) flush_last_page();

    size_t first_page_idx = binary_search_indices(offset);

    // messages from offset is not found
    if (first_page_idx == paged_message_indices_.size()) {
        return Vector<MemBlock>();
    }

    size_t last_page_idx = binary_search_indices(offset + number - 1);
    if (last_page_idx == paged_message_indices_.size()) {
        --last_page_idx;
    }

    uint64_t available_size =
        std::min(offset + number, paged_message_indices_[last_page_idx].total_msg_size()) -
        std::max(offset, paged_message_indices_[first_page_idx].prev_total_msg_size);
    number = std::min(number, available_size);

    Vector<MemBlock> msgs;
    msgs.reserve(number);

    for (size_t page_idx = first_page_idx; page_idx <= last_page_idx; ++page_idx) {
        auto& index = paged_message_indices_[page_idx];
        // read all messages to msgs
        data_file_->read(index.page_offset,
                         [this, &index, &offset, &number, &msgs](const char* ptr) {
                             this->read_msgs(index, offset, number, ptr, msgs);
                         });
    }

    return msgs;
}

QueueStore::QueueStore(const String& location) : location_(location) {
    // load all data files
    for (int i = 0; i < DATA_FILE_SPLITS; ++i) {
        data_files_[i] = new PagedFile(data_file_path(i));
    }

    // currently loading from index file is disabled.
    /* load_queues_metadatas(); */
}

QueueStore::~QueueStore() {
    flush_queues_metadatas();

    for (int i = 0; i < DATA_FILE_SPLITS; ++i) {
        delete data_files_[i];
    }
}

void QueueStore::load_queues_metadatas() {
    int fd = ::open(index_file_path().c_str(), O_RDONLY);
    if (fd < 0) return;

    char header_buf[sizeof(MessageQueue::MessageQueueIndexHeader)];
    while (::read(fd, header_buf, sizeof(header_buf)) == sizeof(header_buf)) {
        MessageQueue::MessageQueueIndexHeader header;
        buffer::read_from_buf(header_buf, header);
        char extra_buf[header.extra_length()];
        if (::read(fd, extra_buf, sizeof(extra_buf)) != (ssize_t)sizeof(extra_buf)) return;

        // set up queue
        SharedPtr<MessageQueue> q = std::make_shared<MessageQueue>();
        q->load_queue_metadata(header, extra_buf);
        q->set_data_file(data_files_[q->get_queue_id() % DATA_FILE_SPLITS]);

        // add this queue
        queues_.insert(std::make_pair(q->get_queue_name(), q));
    }
}

void QueueStore::flush_queues_metadatas() {
    int fd = ::open(index_file_path().c_str(), O_RDWR | O_CREAT | O_TRUNC, 0644);
    assert(fd > 0);
    for (auto& entry : queues_) {
        auto& q = entry.second;
        q->flush_last_page();
        q->flush_queue_metadata(fd);
    }
}

SharedPtr<MessageQueue> QueueStore::find_or_create_queue(const String& queue_name) {
    ConcurrentHashMap<String, SharedPtr<MessageQueue>>::const_accessor ac;
    if (!queues_.find(ac, queue_name)) {
        uint32_t queue_id = next_queue_id_.next();
        SharedPtr<MessageQueue> queue_ptr = std::make_shared<MessageQueue>(
            queue_id, queue_name, data_files_[queue_id % DATA_FILE_SPLITS]);
        queues_.insert(ac, std::make_pair(queue_name, queue_ptr));
        DLOG("Created a new queue, id: %d, name: %s", q->get_queue_id(),
             q->get_queue_name().c_str());
    }
    return ac->second;
}

SharedPtr<MessageQueue> QueueStore::find_queue(const String& queue_name) const {
    ConcurrentHashMap<String, SharedPtr<MessageQueue>>::const_accessor ac;
    auto found = queues_.find(ac, queue_name);
    return found ? ac->second : nullptr;
}

void QueueStore::put(const String& queue_name, const MemBlock& message) {
    if (!message.ptr) return;
    auto q_ptr = find_or_create_queue(queue_name);
    q_ptr->put(message);
}

Vector<MemBlock> QueueStore::get(const String& queue_name, long offset, long size) {
    auto q_ptr = find_queue(queue_name);
    if (q_ptr) return q_ptr->get(offset, size);
    // return empty list when queue is not found
    return Vector<MemBlock>();
}
}  // namespace race2018
