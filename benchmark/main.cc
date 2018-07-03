#include "queue_store.h"

#include <algorithm>
#include <cstdio>
#include <cstdlib>
#include <map>
#include <random>
#include <string>
#include <thread>

using namespace std;
using namespace race2018;

random_device rd;
uniform_int_distribution<int> dist(50, 64);

const int QUEUE_NUM = 10000;
const int MESSAGE_NUM = 2000;

void generate_random_string(char *dest, int len) {
    static const char alphanum[] =
        "0123456789"
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        "abcdefghijklmnopqrstuvwxyz";

    static uniform_int_distribution<int> str_dist(0, sizeof(alphanum) - 1);

    for (int i = 0; i < len; ++i) {
        dest[i] = alphanum[i % sizeof(alphanum)];
    }
}

void produce_messages(queue_store &store, int idx, int thread_num) {
    for (int msg_idx = 0; msg_idx < MESSAGE_NUM; ++msg_idx) {
        for (int i = idx; i < QUEUE_NUM; i += thread_num) {
            MemBlock msg;
            msg.size = 50 + rand() % 15;
            msg.ptr = (char *)::malloc(msg.size + 1);
            int head_len = sprintf((char *)msg.ptr, "q%d-%d-", i, msg_idx);
            int msg_len = msg.size - head_len - 3;
            generate_random_string((char *)msg.ptr + head_len, msg_len);
            memcpy((char *)msg.ptr + head_len + msg_len, "-ed", 3);
            *((char *)(msg.ptr) + msg.size) = '\0';
            // now msg is {queue name}-{idx}-{random message}-ed
            store.put("Queue-" + to_string(i), msg);
        }
    }
}

void start_benckmark(int thread_num) {
    queue_store store;
    std::thread producers[thread_num];
    for (int i = 0; i < thread_num; ++i) {
        producers[i] =
            std::thread([&store, i, thread_num]() { produce_messages(store, i, thread_num); });
    }
    for (int i = 0; i < thread_num; ++i) {
        producers[i].join();
    }

    uniform_int_distribution<int> queue_dist(0, QUEUE_NUM - 1);
    int queue_id = queue_dist(rd);
    string queue_name = "Queue-" + to_string(queue_id);
    uniform_int_distribution<int> msg_dist(0, MESSAGE_NUM - 1);
    int begin = msg_dist(rd), end = msg_dist(rd);
    if (begin > end) swap(begin, end);
    LOG("Try to read all messages of %s, msg offset: %d, msg size: %d", queue_name.c_str(), begin,
        end - begin + 1);
    vector<MemBlock> msgs = store.get(queue_name, begin, end - begin + 1);

    LOG("Log all read messages, size: %ld", msgs.size());
    char header[100];
    for (auto &msg : msgs) {
        printf("size: %ld, msg: %s\n", msg.size, msg.ptr);

        int hd_len = sprintf(header, "q%d-%d-", queue_id, begin++);
        assert(strncmp(header, (char *)msg.ptr, hd_len) == 0);
        assert(strncmp((char *)msg.ptr + msg.size - 3, "-ed", 3) == 0);

        free(msg.ptr);
    }

    // TODO implement index checker and consumers
    std::thread index_checkers[thread_num];
    std::thread consumer[thread_num];
}

int main(int argc, char *argv[]) {
    std::srand(std::time(nullptr));
    start_benckmark(20);
    return 0;
}
