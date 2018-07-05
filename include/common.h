#ifndef QUEUE_RACE_COMMON_H
#define QUEUE_RACE_COMMON_H

#include "logger.h"

#include <algorithm>
#include <atomic>
#include <boost/circular_buffer.hpp>
#include <boost/scoped_ptr.hpp>
#include <cassert>
#include <cstdint>
#include <cstring>
#include <functional>
#include <memory>
#include <queue>
#include <string>
#include <type_traits>
#include <vector>

// tbb
#include <tbb/concurrent_hash_map.h>
#include <tbb/concurrent_priority_queue.h>
#include <tbb/concurrent_queue.h>
#include <tbb/concurrent_unordered_map.h>
#include <tbb/concurrent_vector.h>
#define TBB_PREVIEW_CONCURRENT_LRU_CACHE 1
#include <tbb/concurrent_lru_cache.h>

namespace race2018 {

// Basic containers
using String = std::string;
template <class T>
using Vector = std::vector<T>;
template <class T>
using Queue = std::queue<T>;
template <class K, class V>
using Pair = std::pair<K, V>;
template <class T>
using CircularBuffer = boost::circular_buffer<T>;

// Smart pointers
template <class T>
using SharedPtr = std::shared_ptr<T>;
template <class T>
using UniquePtr = std::unique_ptr<T>;
template <class T>
using ScopedPtr = boost::scoped_ptr<T>;

// Atomic types
template <class T>
using Atomic = std::atomic<T>;

// Concurrent containers
template <class K, class V, class C = tbb::tbb_hash_compare<K>>
using ConcurrentHashMap = tbb::concurrent_hash_map<K, V, C>;
template <class K, class V>
using ConcurrentUnorderedMap = tbb::concurrent_unordered_map<K, V>;
template <class T>
using ConcurrentVector = tbb::concurrent_vector<T>;
template <class T>
using ConcurrentQueue = tbb::concurrent_queue<T>;
// template <class K, class V>
// using ConcurrentLruCache = tbb::concurrent_lru_cache<K, V, typename std::function<V(K)>>;
}  // namespace race2018

#endif  // QUEUE_RACE_COMMON_H
