#include "persistence.h"
#include "zmalloc.h"

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
static int _unpackCmd(const char* rbuf, int rbufLen, CmdArgv** cmdArgvs, redisCommandProc** procPtr, int* time);
static void _wait(PMgr* this);
static int _createMainWorkerProcess(PMgr* this);
static int _createWriteWorkerProcess(int num, PMgr* this);

static void* _persistenceMain(void* arg)
{
    PMgr* this = (PMgr*)arg;
    int currWorkerIdx = 0;
    while (1) {
        char* recv;
        int len = popJobList(this->joblist, &recv);
        if (len <= 0) {
            _wait(this);
        } else {
            while (this->writeWorkers[currWorkerIdx]->buflen > 0) {
                if (currWorkerIdx++ == this->workerNum - 1) {
                    currWorkerIdx = 0;
                    _wait(this);
                }
            }
            memcpy(this->writeWorkers[currWorkerIdx]->buf, recv, len);
            this->writeWorkers[currWorkerIdx]->buflen = len;
            incJoblistRsize(this->joblist, len);
        }
    }
    return NULL;
}

PMgr* initPersistence(int joblistsize, const char* mmapFile, int threadNum, const char* host, const int port, const char* user, const char* pwd, const char* dbName)
{
    PMgr* this = (PMgr*)zmalloc(sizeof(PMgr));
    this->workerNum = threadNum;
    this->sleepSum = 0;
    this->joblist = initJoblist(MAX_PERSISTENCE_BUF_SIZE, joblistsize, mmapFile);
    assert(this->joblist != NULL);
    this->writeWorkers = (WriteWorker**)zmalloc(sizeof(WriteWorker*) * threadNum);
    int i = 0;
    for (; i < threadNum; i++) {
        this->writeWorkers[i] = (WriteWorker*)zmalloc(sizeof(WriteWorker) + MAX_PERSISTENCE_BUF_SIZE);
        this->writeWorkers[i]->dbConn = initDB(host, port, user, pwd, dbName);
        this->writeWorkers[i]->buflen = 0;
    }
    int ret = _createMainWorkerProcess(this);
    assert(ret == PERSISTENCE_RET_SUCCESS);
    for (i = 0; i < threadNum; i++) {
        ret = _createWriteWorkerProcess(i, this);
        assert(ret == PERSISTENCE_RET_SUCCESS);
    }
    return this;
}

static int _createMainWorkerProcess(PMgr* this)
{
    pthread_t thread;
    pthread_attr_t attr;
    pthread_attr_init(&attr);
    pthread_create(&thread, &attr, _persistenceMain, this);
    return PERSISTENCE_RET_SUCCESS; 
}

static int _createWriteWorkerProcess(int num, PMgr* this)
{
    pthread_t thread;
    pthread_attr_t attr;
    pthread_attr_init(&attr);
    this->writeWorkers[num]->pmgr = this;
    pthread_create(&thread, &attr, _writeDBPorcess, this->writeWorkers[num]);
    return PERSISTENCE_RET_SUCCESS;  
}

int packPersistenceJob(redisClient* c, char* wbuf)
{
    if (c->argc >= MAX_CMD_ARGV) {
        return PERSISTENCE_RET_ARGC_OVERFLOW;
    }
    return isPersistenceCmd(c) ? _packCmd(wbuf, c) : PERSISTENCE_RET_NOTFOUNDCMD;
}

int addPersistenceJob(const char* wbuf, int len, PMgr* this)
{
    return len > 0 ? pushJobList(this->joblist, wbuf, len) : JOBLIST_RET_SIZE_OVERFLOW;
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

    int now = (int)time(NULL);
    *(int*)end = now;
    offset += sizeof(int);
    memcpy(end + offset, &c->cmd->proc, sizeof(redisCommandProc*));
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

static int _unpackCmd(const char* rbuf, int rbufLen, CmdArgv** cmdArgvs, redisCommandProc** procPtr, int* time)
{
    const char* end = rbuf;
    int i = 0;
    *time = *(int*)end;
    end += sizeof(int);
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

void persistenceInfo(int* untreatedSize, int* sleepSum, unsigned long long * wSize, unsigned long long * rSize, PMgr* this)
{
    *untreatedSize = this != NULL ? this->joblist->jobbuff->wSize - this->joblist->jobbuff->rSize : -1;
    *sleepSum = this != NULL ? this->sleepSum : -1;
    *wSize = this != NULL ? this->joblist->jobbuff->wSize : -1;
    *rSize = this != NULL ? this->joblist->jobbuff->rSize : -1;
}

static void _wait(PMgr* this)
{
    usleep(1000);
    this->sleepSum += 1;
    if (this->sleepSum >= 2147483640) {
        this->sleepSum = 0;
    }
}

static void* _writeDBPorcess(void* arg)
{
    WriteWorker* worker = (WriteWorker*)arg;
    PMgr* this = worker->pmgr;
    while (1) {
        if (worker->buflen == 0) {
            _wait(this);
        } else {
            CmdArgv* cmdArgvs[MAX_CMD_ARGV];
            redisCommandProc* proc;
            int time = 0;
            int argc = _unpackCmd(worker->buf, worker->buflen, cmdArgvs, &proc, &time);
            assert(argc > 0);
            writeToDB(argc, cmdArgvs, proc, worker->dbConn, time);
            worker->buflen = 0;
        }
    }
    return NULL;
}
