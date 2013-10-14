#ifndef __PERSISTENCE_H__
#define __PERSISTENCE_H__

#include "redis.h"
#define MAX_CMD_ARGV 1024
#define JOBLEN_SIZE 4
#define MAX_PERSISTENCE_BUF_SIZE 10240 
#define PERSISTENCE_RET_SIZE_OVERFLOW -1
#define PERSISTENCE_RET_ARGC_OVERFLOW -2
#define PERSISTENCE_RET_NOTFOUNDCMD -3
#define PERSISTENCE_RET_KEYSIZE_EXCEED -4
#define PERSISTENCE_RET_POPJOBLIST_EMPTY -5
#define PERSISTENCE_RET_MMAP_ERROR -6
#define PERSISTENCE_RET_PUSHJOBLIST_SUCCESS 0
#define PERSISTENCE_RET_INIT_SUCCESS 0
#define PERSISTENCE_RET_RESET_SUCCESS 0

#define NEXT_POT(N) do {\
            (N) -= 1; \
            (N) |= (N) >> 16;\
            (N) |= (N) >> 8;\
            (N) |= (N) >> 4;\
            (N) |= (N) >> 2;\
            (N) |= (N) >> 1;\
            (N) += 1;\
        } while (0)

int initPersistence(int joblistsize, const char* mmapFile);
int packPersistenceJob(redisClient* c, char* wbuf);
int addPersistenceJob(const char* wbuf, int len);
int persistenceUntreatedSize(void);
int persistenceWaitSleepSum(void);
#endif
