#ifndef QUEUE_RACE_LOGGER_H
#define QUEUE_RACE_LOGGER_H

#include <cstdio>
#include <cstring>
namespace race2018 {
#define LOG(format, ...) printf(format "\n", __VA_ARGS__)

#ifdef DEBUG
#define DLOG(format, ...) printf(format "\n", __VA_ARGS__)
#else
#define DLOG(format, ...)
#endif
}  // namespace race2018

#endif  // QUEUE_RACE_LOGGER_H
