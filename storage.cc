#include "storage.h"

#include <lz4.h>
#include <snappy.h>
#include <tbb/parallel_for.h>
#include <map>
#include <thread>
#include <gperftools/malloc_extension.h>

namespace race2018 {

void* map_file_page(int fd, size_t offset, bool readonly) {
    void* addr;
    if (readonly) {
        addr = ::mmap(nullptr, FILE_PAGE_SIZE, PROT_READ, MAP_SHARED, fd, offset);
    } else {
        addr = ::mmap(nullptr, FILE_PAGE_SIZE, PROT_WRITE | PROT_READ, MAP_SHARED, fd, offset);
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

void PagedFile::write(uint64_t offset, const char* buf, size_t size) {
    ssize_t ret = ::pwrite(fd_, (const void*)buf, size, offset);
    if (ret == -1) perror("write buffer failed");
}

void PagedFile::read(uint64_t offset, char* buf, size_t size) {
    ssize_t ret = ::pread(fd_, (void*)buf, size, offset);
    if (ret == -1) perror("read buffer failed");
}

void PagedFile::write(const char* buf, size_t size) {
    ssize_t ret = ::write(fd_, (const void*)buf, size);
    if (ret == -1) perror("write buffer failed");
}

void PagedFile::read(char* buf, size_t size) {
    ssize_t ret = ::write(fd_, (void*)buf, size);
    if (ret == -1) perror("read buffer failed");
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

void PagedFile::start_async_readers() {
    bool exp = false;
    if (async_reader_started.compare_exchange_strong(exp, true)) {
        for (int i = 0; i < 4; ++i) {
            async_reader_[i] = std::thread([this]() {
                // handle read requests and read
                AsyncReadRequset arr;
                for (;;) {
                    async_read_requests_.pop(arr);
                    // ignore invalid request
                    if (!arr.cb_queue) continue;

                    read(uint64_t(arr.page_idx) * FILE_PAGE_SIZE, arr.page.get());

                    // try to send back, if failed, destory it
                    arr.cb_queue->try_push(arr.page);
                }
            });
            async_reader_[i].detach();
        }
    }
}

bool PagedFile::async_read(const AsyncReadRequset& request) {
    if (!request.cb_queue) return false;
    if (!async_reader_started.load()) start_async_readers();

    return async_read_requests_.try_push(request);
}

static thread_local bool buffer_allocated = false;
// to protect back_buf from
static thread_local SharedPtr<std::mutex> back_buf_mutex;
const uint32_t BACK_BUF_FREE = 0, BACK_BUF_FLUSH_SCHEDULED = 1, BACK_BUF_FLUSHING = 2;
static thread_local SharedPtr<Atomic<uint32_t>> back_buf_status;
// active is for writing, and back is always free or in flushing
static thread_local SharedPtr<TLSWriteBuffer> active_buf, back_buf;

uint64_t PagedFile::tls_write(const FilePage* page) {
    // initiliaze buffers
    if (!buffer_allocated) {
        back_buf_mutex = std::make_shared<std::mutex>();
        active_buf = std::make_shared<TLSWriteBuffer>(this);
        back_buf = std::make_shared<TLSWriteBuffer>(this);
        back_buf_status = std::make_shared<Atomic<uint32_t>>(0);
        active_buf->allocate_offset();
        buffer_allocated = true;
    }

    uint64_t page_offset = active_buf->write(page);
    if (page_offset == NEGATIVE_OFFSET) {
        // buffer is full, switch to back
        {
            // spin wait until back buf is not in scheduled status
            while (back_buf_status->load() == BACK_BUF_FLUSH_SCHEDULED)
                ;
            // try swap active and back buffer
            std::unique_lock<std::mutex> lock(*back_buf_mutex);
            std::swap(active_buf, back_buf);
            assert(back_buf_status->load() == BACK_BUF_FREE);
            uint32_t exp = BACK_BUF_FREE;
            back_buf_status->compare_exchange_strong(exp, BACK_BUF_FLUSH_SCHEDULED);
        }
        // start a thread to flush full (back) buf
        auto buf_to_flush = back_buf;
        auto buf_mutex_ptr = back_buf_mutex;
        auto status_ptr = back_buf_status;
        std::thread flush_th([buf_to_flush, status_ptr, buf_mutex_ptr]() {
            std::unique_lock<std::mutex> lock(*buf_mutex_ptr);
            uint32_t exp = BACK_BUF_FLUSH_SCHEDULED;
            status_ptr->compare_exchange_strong(exp, BACK_BUF_FLUSHING);
            // going to flush
            buf_to_flush->flush();

            exp = BACK_BUF_FLUSHING;
            status_ptr->compare_exchange_strong(exp, BACK_BUF_FREE);
        });
        flush_th.detach();

        // allocate new offset on active buffer, and write to it
        active_buf->allocate_offset();
        page_offset = active_buf->write(page);
    }
    return page_offset;
}

void PagedFile::tls_flush() {
    if (active_buf) {
        // flush active and wait back to be flushed
        active_buf->flush();

        // spin wait until back buf is not in scheduled status
        while (back_buf_status->load() == BACK_BUF_FLUSH_SCHEDULED)
            ;
        // wait for back to be flushed
        std::unique_lock<std::mutex> lock(*back_buf_mutex);

        back_buf_mutex.reset();
        active_buf.reset();
        back_buf.reset();
        back_buf_status.reset();
        buffer_allocated = false;
    }
}

#ifdef __linux__
bool PagedFile::readahead(uint64_t offset, size_t size) {
    return ::readahead(fd_, offset, size) == 0;
}
#endif

TLSWriteBuffer::TLSWriteBuffer(PagedFile* file) : file_(file) {}

TLSWriteBuffer::~TLSWriteBuffer() {
    if (page_count_ > 0) flush();
}

uint64_t TLSWriteBuffer::write(const FilePage* page) {
    if (page_count_ >= TLS_WRITE_BUFFER_PAGE_SIZE) return NEGATIVE_OFFSET;

    const_cast<FilePage*>(page)->header.offset =
        file_offset_ + uint64_t(page_count_) * FILE_PAGE_SIZE;
    memcpy(buf_ + uint64_t(page_count_) * FILE_PAGE_SIZE, (const void*)page, FILE_PAGE_SIZE);
    ++page_count_;
    return page->header.offset;
}

void TLSWriteBuffer::allocate_offset() {
    file_offset_ = file_->allocate_block(TLS_WRITE_BUFFER_SIZE);
}

void TLSWriteBuffer::flush() {
    std::unique_lock<std::mutex> lock(mutex_);
    assert(page_count_ <= TLS_WRITE_BUFFER_PAGE_SIZE);
    file_->write(file_offset_, buf_, FILE_PAGE_SIZE * uint64_t(page_count_));
    page_count_ = 0;
}

MessageQueue::MessageQueue() : MessageQueue(-1, nullptr) {}

MessageQueue::MessageQueue(uint32_t queue_id, QueueStore* store)
    : queue_id_(queue_id), store_(store) {
    paged_message_indices_.reserve(40);
}

struct __attribute__((__packed__)) MessageQueue::Metadata {
    uint32_t queue_id;
    uint32_t name_size;
    uint16_t slot_offset;
    uint32_t indices_size;

    uint64_t index_size() const { return sizeof(Metadata) + extra_length(); }
    uint64_t extra_length() const { return name_size + indices_size * sizeof(MessagePageIndex); }
};

void MessageQueue::construct_metadata(const String& queue_name, Metadata& metadata) const {
    metadata.queue_id = queue_id_;
    metadata.name_size = queue_name.size();
    metadata.slot_offset = cur_data_slot_off_;
    metadata.indices_size = paged_message_indices_.size();
}

void MessageQueue::flush_queue_metadata(const String& queue_name, int fd) const {
    Metadata metadata;
    construct_metadata(queue_name, metadata);
    size_t buf_len = metadata.index_size();

    char* buf[buf_len];
    buffer::write_to_buf(buf, metadata);
    memcpy(buf + sizeof(Metadata), queue_name.c_str(), metadata.name_size);
    auto ptr = buf + sizeof(Metadata) + metadata.name_size;
    for (auto& index : paged_message_indices_) {
        buffer::write_to_buf(ptr, index);
        ptr += sizeof(index);
    }
    ::write(fd, buf, buf_len);
}

String MessageQueue::load_queue_metadata(const Metadata& metadata, const char* buf) {
    queue_id_ = metadata.queue_id;
    cur_data_slot_off_ = metadata.slot_offset;
    String queue_name = String(buf, metadata.name_size);
    paged_message_indices_.reserve(metadata.indices_size);

    MessagePageIndex msg_index;
    auto ptr = buf + metadata.name_size;
    paged_message_indices_.clear();
    for (uint32_t i = 0; i < metadata.indices_size; ++i) {
        buffer::read_from_buf(ptr, msg_index);
        paged_message_indices_.push_back(msg_index);
        ptr += sizeof(MessagePageIndex);
    }
    return queue_name;
}

bool MessageQueue::next_message_slot(uint16_t& slot_offset, uint16_t size) {
    // first page will hold at most (queue_id / DATA_FILE_SPLITS) % 64 + 1 messages, this make write
    // more average. This leads to 64 timepoints of first flush. I call it flush fast.
    bool flush_fast =
        paged_message_indices_.size() == 1 &&
        paged_message_indices_.back().msg_size >= ((queue_id_ / DATA_FILE_SPLITS) & 0x3f) + 1;

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

    paged_message_indices_.back().file_idx = store_->tls_data_file_idx();
    paged_message_indices_.back().page_idx =
        store_->tls_get_data_file()->tls_write(last_page_) / FILE_PAGE_SIZE;
    if (release) {
        ::free(last_page_);
        last_page_ = nullptr;
    }
}

void MessageQueue::write_to_last_page(const MemBlock& msg, uint16_t slot_offset) {
    write_to_last_page((const char*)msg.ptr, msg.size, slot_offset);
}

void MessageQueue::write_to_last_page(const char* ptr, size_t size, uint16_t slot_offset) {
    if (!last_page_) {
        // lazy allocate page
        last_page_ = new FilePage();
    }
    if (size < 0x80) {
        last_page_->content[slot_offset] = size;
        ::memcpy(last_page_->content + slot_offset + 1, ptr, size);
    } else {
        last_page_->content[slot_offset] = ((size >> 8) | 0x80);
        last_page_->content[slot_offset + 1] = size & 0xff;
        ::memcpy(last_page_->content + slot_offset + 2, ptr, size);
    }
}

#define COMPRESS_USING_SNAPPY(msg, msg_ptr, compressed_size) \
    char msg_ptr[snappy::MaxCompressedLength(message.size)]; \
    size_t compressed_size = 0;                              \
    snappy::RawCompress((const char*)msg.ptr, msg.size, msg_ptr, &compressed_size);

#define COMPRESS_USING_LZ4(msg, msg_ptr, compressed_size) \
    char msg_ptr[LZ4_compressBound(message.size)];        \
    size_t compressed_size =                              \
        LZ4_compress_default((const char*)msg.ptr, msg_ptr, msg.size, sizeof(msg_ptr));

#define COMPRESS_NONE(msg, msg_ptr, compressed_size) \
    char* msg_ptr = (char*)msg.ptr;                  \
    size_t compressed_size = msg.size;

// put is sequential to support index verification
void MessageQueue::put(const MemBlock& message) {
    // maximum support message size of
    assert(message.size <= 4000);

    if (paged_message_indices_.empty()) {
        // create the first index
        paged_message_indices_.emplace_back(NEGATIVE_OFFSET, 0);
    }

    // COMPRESS_USING_SNAPPY(message, msg_ptr, compressed_size);
    // COMPRESS_USING_LZ4(message, msg_ptr, compressed_size);
    COMPRESS_NONE(message, msg_ptr, compressed_size);
    assert(compressed_size > 0 && compressed_size <= 4000);
    DLOG("compressed from %ld to %ld", message.size, compressed_size);

    uint16_t msg_size = compressed_size + 1;
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
    write_to_last_page(msg_ptr, compressed_size, slot_offset);

    // free raw message
    ::free(message.ptr);

    ++paged_message_indices_.back().msg_size;
}

size_t MessageQueue::binary_search_indices(uint32_t msg_offset) const {
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

#define UNCOMPRESS_USING_SNAPPY(_begin, _size, _out_msg)                           \
    size_t raw_len;                                                                \
    snappy::GetUncompressedLength(_begin, _size, &raw_len);                        \
    _out_msg.ptr = new char[raw_len];                                              \
    _out_msg.size = raw_len;                                                       \
    bool uncompressed = snappy::RawUncompress(_begin, _size, (char*)_out_msg.ptr); \
    if (!uncompressed) {                                                           \
    }                                                                              \
    assert(uncompressed);

// FIXME this macro assumes that original message size is not larger than 2048
#define UNCOMPRESS_USING_LZ4(_begin, _size, _out_msg)                              \
    char lz4_out[2048];                                                            \
    size_t raw_len = LZ4_decompress_safe(_begin, lz4_out, _size, sizeof(lz4_out)); \
    assert(raw_len > 0);                                                           \
    _out_msg.ptr = new char[raw_len];                                              \
    _out_msg.size = raw_len;                                                       \
    ::memcpy(_out_msg.ptr, lz4_out, raw_len);

#define UNCOMPRESS_NONE(_begin, _size, _out_msg) _out_msg = new_memblock(_begin, _size);

uint32_t MessageQueue::read_msgs(const MessagePageIndex& index, uint32_t& offset, uint32_t& num,
                                 const char* ptr, Vector<MemBlock>& msgs) {
    uint64_t cur_offset = index.prev_total_msg_size;
    uint16_t size = index.msg_size;
    if (offset >= cur_offset + size) return 0;

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

        MemBlock msg;
        // UNCOMPRESS_USING_LZ4(begin, msg_size, msg);
        // UNCOMPRESS_USING_SNAPPY(begin, msg_size, msg);
        UNCOMPRESS_NONE(begin, msg_size, msg);
        msgs.push_back(msg);
        begin += msg_size;
        ++cur_offset, ++offset, --num, --size;
    }

    return size;
}

static thread_local ConcurrentHashMap<uint32_t, Pair<uint8_t, FilePage>> file_pages;
Vector<MemBlock> MessageQueue::get(uint32_t offset, uint32_t number) {
    decltype(file_pages)::accessor ac;
    file_pages.find(ac, queue_id_);
    if (ac.empty()) {
        file_pages.emplace(ac);
        ac->second.first = 0xff;
    }
    uint8_t& file_idx = ac->second.first;
    FilePage& page = ac->second.second;

    size_t first_page_idx = binary_search_indices(offset);

    // messages from offset is not found
    if (first_page_idx == paged_message_indices_.size()) {
        return Vector<MemBlock>();
    }

    size_t last_page_idx = binary_search_indices(offset + number - 1);
    if (last_page_idx == paged_message_indices_.size()) {
        --last_page_idx;
    }

    uint32_t available_size =
        std::min(offset + number, paged_message_indices_[last_page_idx].total_msg_size()) -
        std::max(offset, paged_message_indices_[first_page_idx].prev_total_msg_size);
    number = std::min(number, available_size);

    Vector<MemBlock> msgs;
    msgs.reserve(number);

    uint16_t msgs_left = 0;
    for (size_t page_idx = first_page_idx; page_idx <= last_page_idx; ++page_idx) {
        auto& index = paged_message_indices_[page_idx];
        // load from data file
        if (file_idx != index.file_idx ||
            page.header.offset != uint64_t(index.page_idx) * FILE_PAGE_SIZE) {
            file_idx = index.file_idx;
            auto data_file_ptr = store_->get_data_file(index.file_idx);
            data_file_ptr->read(uint64_t(index.page_idx) * FILE_PAGE_SIZE, (char*)&page,
                                FILE_PAGE_SIZE);
        }
        // attention, here must be reference!
        assert(page.header.offset == index.page_idx * FILE_PAGE_SIZE);
        msgs_left = read_msgs(index, offset, number, page.content, msgs);
    }

    if (msgs_left <= 10 && last_page_idx + 1 != paged_message_indices_.size()) {
#ifdef __linux__
        auto next_index = paged_message_indices_[last_page_idx + 1];
        auto data_file_ptr = store_->get_data_file(next_index.file_idx);
        data_file_ptr->readahead((uint64_t)next_index.page_idx * FILE_PAGE_SIZE);
#endif
    }

    return msgs;
}

QueueStore::QueueStore(const String& location) : location_(location) {
    // load all data files
    for (int i = 0; i < DATA_FILE_SPLITS; ++i) {
        data_files_[i] = new PagedFile(data_file_path(i));
    }

    LOG("Version: 2018-07-10 1:48pm. Branch: direct_read");

    // currently loading from index file is disabled.
    /* load_queues_metadatas(); */
}

QueueStore::~QueueStore() {
    // flush_queues_metadatas();

    for (int i = 0; i < DATA_FILE_SPLITS; ++i) {
        delete data_files_[i];
    }
}

thread_local int8_t QueueStore::file_idx = -1;
int8_t QueueStore::tls_data_file_idx() {
    if (file_idx < 0) {
        file_idx = next_data_file_idx_.fetch_add(1) % DATA_FILE_SPLITS;
    }
    return file_idx;
}

PagedFile* QueueStore::tls_get_data_file() { return data_files_[tls_data_file_idx()]; }

PagedFile* QueueStore::get_data_file(uint8_t idx) {
    assert(idx < DATA_FILE_SPLITS);
    return data_files_[idx];
}

MessageQueue* QueueStore::find_or_create_queue(const String& queue_name) {
    auto q_ptr = queues_.fast_find(queue_name);
    if (q_ptr) return q_ptr->second;

    decltype(queues_)::const_accessor ac;
    queues_.find(ac, queue_name);
    if (ac.empty()) {
        uint32_t queue_id = next_queue_id_.next();
        auto queue_ptr = new MessageQueue(queue_id, this);
        queues_.insert(ac, std::make_pair(queue_name, queue_ptr));
        DLOG("Created a new queue, id: %d, name: %s", q->get_queue_id(),
             q->get_queue_name().c_str());
    }
    return ac->second;
}

MessageQueue* QueueStore::find_queue(const String& queue_name) const {
    auto q_ptr = queues_.fast_find(queue_name);
    return q_ptr ? q_ptr->second : nullptr;

    decltype(queues_)::const_accessor ac;
    queues_.find(ac, queue_name);
    return ac.empty() ? nullptr : ac->second;
}

void QueueStore::put(const String& queue_name, const MemBlock& message) {
    if (!message.ptr) return;
    auto q_ptr = find_or_create_queue(queue_name);
    q_ptr->put(message);
}

void QueueStore::flush_all_before_read() {
    if (flushed) return;
    std::unique_lock<std::mutex> lock(flush_mutex_);
    if (flushed) return;

    for (auto it = queues_.begin(); it != queues_.end(); ++it) {
        auto mq_ptr = it->second;
        mq_ptr->flush_last_page(true);
    }
    this->tls_get_data_file()->tls_flush();
    // release free memory back to os
    MallocExtension::instance()->ReleaseFreeMemory();
    MallocExtension::instance()->SetMemoryReleaseRate(10.0);

    flushed = true;
}

Vector<MemBlock> QueueStore::get(const String& queue_name, long offset, long size) {
    if (!flushed) flush_all_before_read();

    auto q_ptr = find_queue(queue_name);
    if (q_ptr) return q_ptr->get(offset, size);
    // return empty list when queue is not found
    return Vector<MemBlock>();
}
}  // namespace race2018
