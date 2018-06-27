#include "queue_store.h"
#include <cstring>

using namespace std;
using namespace race2018;

/**
 * This in-memory implementation is for demonstration purpose only. You are supposed to modify it.
 */
void queue_store::put(std::string queue_name, const MemBlock& message) {
    store->put(queue_name, message);
}

/**
 * This in-memory implementation is for demonstration purpose only. You are supposed to modify it.
 */
vector<MemBlock> queue_store::get(std::string queue_name, long offset, long number) {
    return store->get(queue_name, offset, number);
}
