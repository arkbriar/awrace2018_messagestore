#include "storage.h"

#include <thread>
#include "tbb/parallel_for.h"

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
    page.header.offset = offset;
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
    const_cast<FilePage*>(page)->header.offset = offset;
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
    page_ptr->header.offset = offset;
    if (!page_ptr) {
        LOG("fallback to pwrite");
        write(offset, writer);
    } else {
        writer(page_ptr->content);
    }
}

uint64_t PagedFile::next_page_offset() { return page_offset.next(); }

MessageQueue::MessageQueue() { paged_message_indices_.emplace_back(NEGATIVE_OFFSET, 0); }

MessageQueue::MessageQueue(uint32_t queue_id, const String& queue_name, PagedFile* data_file)
    : queue_id_(queue_id), queue_name_(queue_name), data_file_(data_file) {
    paged_message_indices_.emplace_back(NEGATIVE_OFFSET, 0);
}

struct __attribute__((__packed__)) MessageQueue::Metadata {
    uint32_t queue_id;
    uint32_t name_size;
    uint16_t slot_offset;
    uint32_t indices_size;

    uint64_t index_size() const { return sizeof(Metadata) + extra_length(); }
    uint64_t extra_length() const { return name_size + indices_size * sizeof(MessagePageIndex); }
};

void MessageQueue::construct_metadata(Metadata& metadata) const {
    metadata.queue_id = queue_id_;
    metadata.name_size = queue_name_.size();
    metadata.slot_offset = cur_data_slot_off_;
    metadata.indices_size = paged_message_indices_.size();
}

void MessageQueue::flush_queue_metadata(int fd) const {
    Metadata metadata;
    construct_metadata(metadata);
    size_t buf_len = metadata.index_size();

    char* buf[buf_len];
    buffer::write_to_buf(buf, metadata);
    memcpy(buf + sizeof(Metadata), queue_name_.c_str(), metadata.name_size);
    auto ptr = buf + sizeof(Metadata) + metadata.name_size;
    for (auto& index : paged_message_indices_) {
        buffer::write_to_buf(ptr, index);
        ptr += sizeof(index);
    }
    ::write(fd, buf, buf_len);
}

void MessageQueue::load_queue_metadata(const Metadata& metadata, const char* buf) {
    queue_id_ = metadata.queue_id;
    cur_data_slot_off_ = metadata.slot_offset;
    queue_name_ = String(buf, metadata.name_size);
    paged_message_indices_.reserve(metadata.indices_size);

    MessagePageIndex msg_index;
    auto ptr = buf + metadata.name_size;
    paged_message_indices_.clear();
    for (uint32_t i = 0; i < metadata.indices_size; ++i) {
        buffer::read_from_buf(ptr, msg_index);
        paged_message_indices_.push_back(msg_index);
        ptr += sizeof(MessagePageIndex);
    }
}

bool MessageQueue::next_message_slot(uint16_t& slot_offset, uint16_t size) {
    // first page will hold at most (queue_id / DATA_FILE_SPLITS) % 32 + 1 messages, this make write
    // more average. This leads to 32 timepoints of first flush. I call it flush fast.
    bool flush_fast =
        paged_message_indices_.size() == 1 &&
        paged_message_indices_.back().msg_size >= ((queue_id_ / DATA_FILE_SPLITS) & 0x1f) + 1;

    // if page is full or flush fast, then a new page should be allocated
    if (cur_data_slot_off_ + size > FILE_PAGE_AVALIABLE_SIZE || flush_fast) {
        slot_offset = 0;
        cur_data_slot_off_ = size;
        return true;
    } else {
        slot_offset = cur_data_slot_off_;
        cur_data_slot_off_ += size;
        return false;
    }
}

void MessageQueue::flush_last_page(bool release) {
    if (!last_page_) return;

    std::unique_lock<std::mutex> lock(wq_mutex_);

    // allocate a new page and set paged message index correctly
    uint64_t page_offset = data_file_->next_page_offset();
    paged_message_indices_.back().page_offset = page_offset;

    // write all messages in this queue to page of page_offset
    data_file_->write(page_offset, last_page_);
    if (release) {
        delete last_page_;
        last_page_ = nullptr;
    }
}

void MessageQueue::flush_last_page() {
    if (!paged_message_indices_.empty()) {
        flush_last_page(true);
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

    uint16_t slot_offset;
    bool needs_new_page = next_message_slot(slot_offset, msg_size);
    if (needs_new_page) {
        // a new data page should be allocated, and flush messages
        flush_last_page(false);

        // create a new paged message index

        auto& prev_index = paged_message_indices_.back();
        paged_message_indices_.emplace_back(NEGATIVE_OFFSET, prev_index.total_msg_size());
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
    uint16_t msg_size = *(ptr++);
    if (msg_size < 0x80) {
        return msg_size;
    } else {
        return ((msg_size & 0x7f) << 8) + (uint8_t)(*(ptr++));
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

static thread_local ConcurrentHashMap<uint32_t, SharedPtr<FilePage>> reading_pages;

Vector<MemBlock> MessageQueue::get(uint64_t offset, uint64_t number) {
    // flush and release the last writing page
    if (last_page_) flush_last_page();

    // get current thread's reading buffer
    decltype(reading_pages)::const_accessor ac;
    reading_pages.find(ac, queue_id_);
    auto page_ptr = ac.empty() ? nullptr : ac->second;
    if (page_ptr == nullptr) {
        // try allocate one
        page_ptr = SharedPtr<FilePage>(new FilePage());
        if (page_ptr) {
            page_ptr->header.offset = NEGATIVE_OFFSET;
            reading_pages.insert(ac, std::make_pair(queue_id_, page_ptr));
        }
    }

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
        if (page_ptr) {
            if (page_ptr->header.offset != index.page_offset) {
                data_file_->read(index.page_offset, page_ptr.get());
            }
            assert(page_ptr->header.offset == index.page_offset);
            read_msgs(index, offset, number, page_ptr->content, msgs);
        } else {
            // read all messages to msgs
            data_file_->read(index.page_offset,
                             [this, &index, &offset, &number, &msgs](const char* ptr) {
                                 this->read_msgs(index, offset, number, ptr, msgs);
                             });
        }
    }

    // after retriving last message, free current page buffer
    if (offset == paged_message_indices_.back().total_msg_size()) {
        if (page_ptr) reading_pages.erase(ac);
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

    char buf[sizeof(MessageQueue::Metadata)];
    while (::read(fd, buf, sizeof(buf)) == sizeof(buf)) {
        MessageQueue::Metadata metadata;
        buffer::read_from_buf(buf, metadata);
        char extra_buf[metadata.extra_length()];
        if (::read(fd, extra_buf, sizeof(extra_buf)) != (ssize_t)sizeof(extra_buf)) return;

        // set up queue
        SharedPtr<MessageQueue> q = std::make_shared<MessageQueue>();
        q->load_queue_metadata(metadata, extra_buf);
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

void QueueStore::sweep_caches() {
    bool expected = false;
    if (cache_cleared.compare_exchange_strong(expected, true)) {
        std::thread sweeper([this]() {
            size_t grainsize = queues_.size() / std::thread::hardware_concurrency();
            using RangeType = ConcurrentHashMap<String, SharedPtr<MessageQueue>>::const_range_type;
            tbb::parallel_for(queues_.range(grainsize), [](const RangeType& r) {
                for (auto it = r.begin(); it != r.end(); ++it) {
                    auto mq_ptr = it->second;
                    mq_ptr->flush_last_page();
                }
            });
        });
        sweeper.detach();
    }
}

void QueueStore::put(const String& queue_name, const MemBlock& message) {
    if (!message.ptr) return;
    // mark cache's not cleared
    cache_cleared.store(false);

    auto q_ptr = find_or_create_queue(queue_name);
    q_ptr->put(message);
}

Vector<MemBlock> QueueStore::get(const String& queue_name, long offset, long size) {
    // sweep all queues' writing cache
    if (!cache_cleared.load()) sweep_caches();

    auto q_ptr = find_queue(queue_name);
    if (q_ptr) return q_ptr->get(offset, size);
    // return empty list when queue is not found
    return Vector<MemBlock>();
}
}  // namespace race2018
