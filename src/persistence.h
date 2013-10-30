#ifndef __PERSISTENCE_H__
#define __PERSISTENCE_H__

#include "redis.h"
#include "mysqlDB.h"
#include "joblist.h"
#define MAX_CMD_ARGV 1024
#define MAX_PERSISTENCE_BUF_SIZE 1024 
#define PERSISTENCE_RET_ARGC_OVERFLOW -2
#define PERSISTENCE_RET_NOTFOUNDCMD -3
#define PERSISTENCE_RET_KEYSIZE_EXCEED -4
#define PERSISTENCE_RET_MMAP_ERROR -6
#define PERSISTENCE_RET_SUCCESS 0
struct _PMgr;

typedef struct _WriteWorker {
    int buflen;
    DBConn* dbConn;
    struct _PMgr* pmgr;
    char buf[];
} WriteWorker;

typedef struct _PMgr {
    JobList* joblist;
    int sleepSum;
    int workerNum;
    WriteWorker** writeWorkers;
} PMgr;

PMgr* initPersistence(int joblistsize, const char* mmapFile, int threadNum, const char* host, const int port, const char* user, const char* pwd, const char* dbName);
int packPersistenceJob(redisClient* c, char* wbuf);
int addPersistenceJob(const char* wbuf, int len, PMgr* this);
void persistenceInfo(int* untreatedSize, int* sleepSum, unsigned long long * wSize, unsigned long long * rSize, PMgr* this);
#endif
