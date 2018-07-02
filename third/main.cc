#include <boost/timer/timer.hpp>
#include <chrono>
#include <csignal>
#include <cstring>
#include <iostream>
#include <ratio>
#include <thread>
#include <vector>

#include "queue_store.h"

using namespace std;
using namespace race2018;

//发送阶段的发送数量，也即发送阶段必须要在规定时间内把这些消息发送完毕方可
int msgNum = 10000000;
//队列的数量
const int queueNum = 1000;

//每个队列发送的数据量
const int queueMsgNum = msgNum / queueNum;

//正确性检测的次数
const int checkNum = 1000;
//消费阶段的总队列数量
const int checkQueueNum = 1000;
//发送的线程数量
const int sendTsNum = 10;
//消费的线程数量
const int checkTsNum = 10;
//消费的线程数量
const int consumerTsNum = 10;

// 每个queue随机检查次数
const int perQueueCheckNum = 10;

const std::string messagePrefix = "12345678901234567890123456789012345678901234567890-%d-%d";
const std::string queueNamePrefix = "abc123-wyp-";

queue_store store;

//#define DEBUG_FLAG

void CheckMessageCount(int offset, int count, size_t realCount) {
    size_t expectCount = std::min(queueMsgNum - offset, count);
    if (expectCount != realCount) {
        printf("[##ERROR##] check message count error, %d %d %d %d\n", offset, count,
               (int)realCount, (int)expectCount);
    }
}

void CheckSingleMemBlock(int queueIndex, int num, const MemBlock &memBlock) {
    char readlMessage[256];
    size_t msgSize = sprintf(readlMessage, messagePrefix.c_str(), queueIndex, num);
    if (msgSize != memBlock.size) {
        printf("[##ERROR##] check message size error, %d %d %d %d\n", queueIndex, num, (int)msgSize,
               (int)memBlock.size);
        return;
    }
    if (memcmp(memBlock.ptr, readlMessage, msgSize) != 0) {
        printf("[##ERROR##] check message content error, %d %d %s \n \t\t\t %s\n", queueIndex, num,
               readlMessage, string((const char *)memBlock.ptr, memBlock.size).c_str());
        return;
    }
#ifdef DEBUG_FLAG
    printf("%s\n", readlMessage);
#endif
}

void CheckResult(int queueIndex, int offset, int count, const std::vector<MemBlock> blockList) {
    CheckMessageCount(offset, count, blockList.size());
    for (size_t size = 0; size < blockList.size(); ++size) {
        CheckSingleMemBlock(queueIndex, offset + size, blockList[size]);
        delete[]((char *)blockList[size].ptr);
    }
}

MemBlock GenerateMemBlock(int queueIndex, int num) {
    MemBlock memBlock;
    memBlock.ptr = new char[messagePrefix.size() + 20];
    memBlock.size = sprintf((char *)memBlock.ptr, messagePrefix.c_str(), queueIndex, num);
    return memBlock;
}

void SendFun(int startQueue, int endQueue) {
    for (int num = 0; num < queueMsgNum; ++num) {
        for (int queueIndex = startQueue; queueIndex < endQueue; ++queueIndex) {
            store.put(queueNamePrefix + to_string(queueIndex), GenerateMemBlock(queueIndex, num));
        }
    }
}

void RandCheck(vector<int> queueIndexList, int checkCount) {
    for (size_t index = 0; index < queueIndexList.size(); ++index) {
        for (int i = 0; i < checkCount; ++i) {
            int offset = random() % queueMsgNum;
            int msgCount = random() % 10 + 5;
            int queueIndex = queueIndexList[index];
            auto result = store.get(queueNamePrefix + to_string(queueIndex), offset, msgCount);
            CheckResult(queueIndex, offset, msgCount, result);
        }
    }
}

void ConsumeCheck(vector<int> queueIndexList) {
    for (size_t index = 0; index < queueIndexList.size(); ++index) {
        int startOffset = 0;
        while (startOffset < queueMsgNum) {
            int msgCount = random() % 10 + 5;
            int queueIndex = queueIndexList[index];
            auto result = store.get(queueNamePrefix + to_string(queueIndex), startOffset, msgCount);
            CheckResult(queueIndex, startOffset, msgCount, result);
            startOffset += result.size();
        }
    }
}

int main(int argc, char *argv[]) {
    using namespace std::chrono;
    {
        high_resolution_clock::time_point t1 = high_resolution_clock::now();
        boost::timer::auto_cpu_timer t;
        vector<std::thread> producerThreads;
        for (int i = 0; i < sendTsNum; ++i) {
            int startQueue = i * queueNum / sendTsNum;
            int endQueue = startQueue + queueNum / sendTsNum;
            producerThreads.push_back(std::thread(SendFun, startQueue, endQueue));
        }

        for (int i = 0; i < sendTsNum; ++i) {
            producerThreads[i].join();
        }
        high_resolution_clock::time_point t2 = high_resolution_clock::now();
        duration<double, std::ratio<1, 1>> duration_s(t2 - t1);
        std::cout << duration_s.count() << " seconds" << std::endl;
    }

    {
        boost::timer::auto_cpu_timer t;
        vector<std::thread> randomCheckThreads;
        for (int i = 0; i < checkTsNum; ++i) {
            vector<int> queueIndexList;
            int thisThreadCheckNum = checkNum / checkTsNum;
            for (int j = 0; j < thisThreadCheckNum; ++j) {
                queueIndexList.push_back(random() % queueNum);
            }
            randomCheckThreads.push_back(std::thread(RandCheck, queueIndexList, perQueueCheckNum));
        }

        for (int i = 0; i < checkTsNum; ++i) {
            randomCheckThreads[i].join();
        }
    }

    {
        boost::timer::auto_cpu_timer t;
        vector<std::thread> consumerThreads;
        for (int i = 0; i < consumerTsNum; ++i) {
            int thisThreadConsumeNum = checkQueueNum / consumerTsNum;
            vector<int> queueIndexList;
            for (int j = 0; j < thisThreadConsumeNum; ++j) {
                queueIndexList.push_back(random() % queueNum);
            }
            consumerThreads.push_back(std::thread(ConsumeCheck, queueIndexList));
        }
        for (int i = 0; i < consumerTsNum; ++i) {
            consumerThreads[i].join();
        }
        //::raise(SIGUSR1);
        // consumerThreads[1000000].join();
    }

    printf("[Done]....");

    return 0;
}
