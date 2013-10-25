#include "mysqlDB.h"
#include "persistence.h"
#include "zmalloc.h"
#include "joblist.h"

#include <assert.h>
#include <unistd.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <pthread.h>
#include <sys/types.h>
#include <sys/stat.h>

static void* _persistenceMain(void* arg);
static void* _writeDBPorcess(void* arg);
static int _packCmd(char* wbuf, redisClient* c);
static int _unpackCmd(const char* rbuf, int rbufLen, CmdArgv** argv, redisCommandProc** procPtr);
static void _wait(void);
static int _createMainWorkerProcess(void);
static int _createWriteWorkerProcess(int num);

typedef struct _PMgr {
    JobList* joblist;
    int sleepSum;
    int workerNum;
} PMgr;

typedef struct _WriteWorker {
    int buflen;
    DBConn* dbConn;
    char buf[];
} WriteWorker;

PMgr* pmgr;
WriteWorker** writeWorkers;

static void* _persistenceMain(void* arg)
{
    int currWorkerIdx = 0;
    while (1) {
        char* recv;
        int len = popJobList(pmgr->joblist, &recv);
        if (len <= 0) {
            _wait();
        } else {
            while (writeWorkers[currWorkerIdx]->buflen > 0) {
                if (currWorkerIdx++ == pmgr->workerNum - 1) {
                    currWorkerIdx = 0;
                    _wait();
                }
            }
            memcpy(writeWorkers[currWorkerIdx]->buf, recv, len);
            writeWorkers[currWorkerIdx]->buflen = len;
            incJoblistRsize(pmgr->joblist, len);
        }
    }
    return NULL;
}

int initPersistence(int joblistsize, const char* mmapFile, int threadNum, const char* host, const int port, const char* user, const char* pwd, const char* dbName)
{
    pmgr = (PMgr*)zmalloc(sizeof(PMgr));
    pmgr->workerNum = threadNum;
    pmgr->sleepSum = 0;
    pmgr->joblist = initJoblist(MAX_PERSISTENCE_BUF_SIZE, joblistsize, mmapFile);
    assert(pmgr->joblist != NULL);
    writeWorkers = (WriteWorker**)zmalloc(sizeof(WriteWorker*) * threadNum);
    int i = 0;
    for (; i < threadNum; i++) {
        writeWorkers[i] = (WriteWorker*)zmalloc(sizeof(WriteWorker) + MAX_PERSISTENCE_BUF_SIZE);
        writeWorkers[i]->dbConn = initDB(host, port, user, pwd, dbName);
        writeWorkers[i]->buflen = 0;
    }
    int ret = _createMainWorkerProcess();
    assert(ret == PERSISTENCE_RET_SUCCESS);
    for (i = 0; i < threadNum; i++) {
        ret = _createWriteWorkerProcess(i);
        assert(ret == PERSISTENCE_RET_SUCCESS);
    }
    return PERSISTENCE_RET_SUCCESS;
}

static int _createMainWorkerProcess(void)
{
    pthread_t thread;
    pthread_attr_t attr;
    pthread_attr_init(&attr);
    pthread_create(&thread, &attr, _persistenceMain, NULL);
    return PERSISTENCE_RET_SUCCESS; 
}

static int _createWriteWorkerProcess(int num)
{
    pthread_t thread;
    pthread_attr_t attr;
    pthread_attr_init(&attr);
    pthread_create(&thread, &attr, _writeDBPorcess, writeWorkers[num]);
    return PERSISTENCE_RET_SUCCESS;  
}

int packPersistenceJob(redisClient* c, char* wbuf)
{
    if (c->argc >= MAX_CMD_ARGV) {
        return PERSISTENCE_RET_ARGC_OVERFLOW;
    }
    int argc = c->argc - 1;
    if ((c->cmd->proc == setCommand && argc == 2)
        || (c->cmd->proc == msetCommand && argc % 2 == 0)
        || (c->cmd->proc == expireatCommand && argc == 2)
        || (c->cmd->proc == lpopCommand && argc == 1)
        || (c->cmd->proc == rpopCommand && argc == 1)
        || (c->cmd->proc == lpushxCommand && argc == 2)
        || (c->cmd->proc == rpushxCommand && argc == 2)
        || (c->cmd->proc == lpushCommand && argc >= 2)
        || (c->cmd->proc == rpushCommand && argc >= 2)
        || (c->cmd->proc == zaddCommand && argc % 2 == 1)
        || (c->cmd->proc == incrCommand && argc == 1)
        || (c->cmd->proc == incrbyCommand && argc == 2)
        || (c->cmd->proc == zincrbyCommand && argc == 3)
        || (c->cmd->proc == zremCommand && argc >= 2)
        || (c->cmd->proc == zremrangebyscoreCommand && argc == 3)
        || (c->cmd->proc == zremrangebyrankCommand && argc == 3)
       ) {
        return _packCmd(wbuf, c);
    }
    return PERSISTENCE_RET_NOTFOUNDCMD;
}

int addPersistenceJob(const char* wbuf, int len)
{
    return len > 0 ? pushJobList(pmgr->joblist, wbuf, len) : JOBLIST_RET_SIZE_OVERFLOW;
}

static int _packCmd(char* wbuf, redisClient* c)
{
    int n = 1;
    int keylen = strlen(c->argv[1]->ptr);
    if (keylen >= MAX_KEY_LEN) {
        return PERSISTENCE_RET_KEYSIZE_EXCEED;
    }
    int offset = 0;
    char* end = wbuf;
    redisLog(REDIS_DEBUG, "packCmd proc %p ", c->cmd->proc);
    memcpy(end, &c->cmd->proc, sizeof(redisCommandProc*));
    offset += sizeof(redisCommandProc*);
    for (; n < c->argc; n++) {
        end = wbuf + offset;
        CmdArgv* cmdArgv = (CmdArgv*)end;
        cmdArgv->len = strlen(c->argv[n]->ptr);
        offset += cmdArgv->len + sizeof(cmdArgv->len);
        if (offset >= MAX_PERSISTENCE_BUF_SIZE) {
            return JOBLIST_RET_SIZE_OVERFLOW;
        }
        memcpy(cmdArgv->buf, c->argv[n]->ptr, cmdArgv->len);
    }
    return offset;
}

static int _unpackCmd(const char* rbuf, int rbufLen, CmdArgv** cmdArgvs, redisCommandProc** procPtr)
{
    const char* end = rbuf;
    int i = 0;
    memcpy(procPtr, end, sizeof(redisCommandProc*));
    redisLog(REDIS_DEBUG, "unpackCmd proc %p ", *procPtr);
    end += sizeof(redisCommandProc*);
    while ((end - rbuf) < rbufLen) {
        cmdArgvs[i] = (CmdArgv*)end;
        end += sizeof(cmdArgvs[i]->len) + cmdArgvs[i++]->len;
        assert(i <= MAX_CMD_ARGV);
    }
    return i;
}

void persistenceInfo(int* untreatedSize, int* sleepSum, unsigned long long * wSize, unsigned long long * rSize)
{
    *untreatedSize = pmgr != NULL ? pmgr->joblist->jobbuff->wSize - pmgr->joblist->jobbuff->rSize : -1;
    *sleepSum = pmgr != NULL ? pmgr->sleepSum : -1;
    *wSize = pmgr->joblist->jobbuff->wSize;
    *rSize = pmgr->joblist->jobbuff->rSize;
}

static void _wait(void)
{
    usleep(1000);
    pmgr->sleepSum += 1;
    if (pmgr->sleepSum >= 2147483640) {
        pmgr->sleepSum = 0;
    }
}

static void* _writeDBPorcess(void* arg)
{
    WriteWorker* worker = (WriteWorker*)arg;
    while (1) {
        if (worker->buflen == 0) {
            _wait();
        } else {
            CmdArgv* cmdArgvs[MAX_CMD_ARGV];
            redisCommandProc* proc;
            int argc = _unpackCmd(worker->buf, worker->buflen, cmdArgvs, &proc);
            assert(argc > 0);
            writeToDB(argc, cmdArgvs, proc, worker->dbConn);
            worker->buflen = 0;
        }
    }
    return NULL;
}
