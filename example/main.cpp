#include <iostream>
#include <cstring>
#include "queue_store.h"

using namespace std;
using namespace race2018;

int main(int argc, char* argv[]) {
    queue_store store;

    for (int i = 0; i < 16; i++) {
        string slogan = string("abc") + to_string(i);
        char* data = new char[slogan.size() + 1];
        strcpy(data, slogan.c_str());
        MemBlock msg = {static_cast<void*>(data), static_cast<size_t>(slogan.size())};

        store.put("Queue-1", msg);
    }
    // test 1024 msg
    MemBlock msg;
    char* data = new char[1024];
    for (int i = 0; i < 1020; ++i) {
        data[i] = 'a';
    }
    memcpy(data + 1020, "1024", 4);
    msg.ptr = data;
    msg.size = 1024;
    store.put("Queue-1", msg);

    vector<MemBlock> list = store.get("Queue-1", 10, 10);

    for (MemBlock& item : list) {
        char* msg = static_cast<char*>(item.ptr);
        cout << "size: " << item.size << ", msg: " << msg << endl;
    }

    return 0;
}
