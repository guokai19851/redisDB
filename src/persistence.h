#ifndef __PERSISTENCE_H__
#define __PERSISTENCE_H__

#include "redis.h"
#define MAX_CMD_ARGV 1024
#define MAX_PERSISTENCE_BUF_SIZE 64 
#define PERSISTENCE_RET_ARGC_OVERFLOW -2
#define PERSISTENCE_RET_NOTFOUNDCMD -3
#define PERSISTENCE_RET_KEYSIZE_EXCEED -4
#define PERSISTENCE_RET_MMAP_ERROR -6
#define PERSISTENCE_RET_SUCCESS 0

int initPersistence(int joblistsize, const char* mmapFile, int threadNum, const char* host, const int port, const char* user, const char* pwd, const char* dbName);
int packPersistenceJob(redisClient* c, char* wbuf);
int addPersistenceJob(const char* wbuf, int len);
void persistenceInfo(int* untreatedSize, int* sleepSum, unsigned long long * wsize, unsigned long long * rsize);
#endif
