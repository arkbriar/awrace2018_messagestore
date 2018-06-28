#ifndef QUEUE_RACE_COMMON_H
#define QUEUE_RACE_COMMON_H

#include <algorithm>
#include <atomic>
#include <boost/scoped_ptr.hpp>
#include <cassert>
#include <cstdint>
#include <cstring>
#include <memory>
#include <string>
#include <vector>

// tbb
#include <tbb/concurrent_hash_map.h>
#include <tbb/concurrent_vector.h>
// patch concurrent lru cache for tbb 3.2
#include "tbbpatch/concurrent_lru_cache.h"

#include "logger.h"

namespace race2018 {

// Basic containers
using String = std::string;
template <class T>
using Vector = std::vector<T>;

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
template <class K, class V>
using ConcurrentHashMap = tbb::concurrent_hash_map<K, V>;
template <class K, class V>
using ConcurrentHashMapAccessor = typename ConcurrentHashMap<K, V>::accessor;
template <class K, class V>
using ConstConcurrentHashMapAccessor = typename ConcurrentHashMap<K, V>::const_accessor;
template <class T>
using ConcurrentVector = tbb::concurrent_vector<T>;
template <class K, class V>
using ConcurrentLruCache = tbb::concurrent_lru_cache<K, V>;
}  // namespace race2018

#endif  // QUEUE_RACE_COMMON_H
