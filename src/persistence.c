#include "mysqlDB.h"
#include "persistence.h"
#include "zmalloc.h"

#include <unistd.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <pthread.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/stat.h>

#define FILE_MODE (S_IRUSR | S_IWUSR)

static int _pushJobList(const char* wbuf, int len);
static int _resizeJobListStart(void);
static int _resizeJobListEnd(void);
static int _popJobList(char** rbuf);
static int _initJoblist(int joblistsize);
static void _incJoblistWriteidx(int inc);
static void _incJoblistReadidx(int inc);
static void* _persistenceMain(void* arg);
static int _packCmd(char* wbuf, redisClient* c);
static int _unpackCmd(const char* rbuf, int rbufLen, CmdArgv** argv, redisCommandProc** procPtr);
static void _waitSleep(void);

//单读单写非阻塞队列
typedef struct _JobList {
    int widx;
    int ridx;
    char buf[];
} JobList;

typedef struct _PMgr {
    JobList* joblist;
    int joblistsize;
    int resize;
    int sleepSum;
    const char* mmapFile;
} PMgr;

PMgr* pmgr;

static int _initJoblist(int joblistsize)
{
    NEXT_POT(joblistsize);
    int flags = O_RDWR | O_CREAT; 
    int exist = access(pmgr->mmapFile, F_OK) == 0; 
    int filesize = joblistsize + sizeof(JobList);
    int mmapFd = open(pmgr->mmapFile, flags, FILE_MODE);
    if (mmapFd <= 0) {
        redisLog(REDIS_WARNING, "create mmap file error %s, %d", pmgr->mmapFile, mmapFd);
        return PERSISTENCE_RET_MMAP_ERROR;
    }
    if (exist) {
        struct stat s;
        stat(pmgr->mmapFile, &s);
        filesize = s.st_size > filesize ? s.st_size : filesize;
    }
    ftruncate(mmapFd, filesize);
    pmgr->joblist = (JobList*)mmap(NULL, filesize, PROT_READ | PROT_WRITE, MAP_SHARED, mmapFd, 0);
    if (!exist) {
        pmgr->joblist->ridx = pmgr->joblist->widx = 0;
    }
    pmgr->joblistsize = filesize - sizeof(JobList);
    redisLog(REDIS_WARNING, "persistence joblist widx:%d\t ridx:%d\t len:%d resize:%d \tsleepSum:%d", pmgr->joblist->widx, pmgr->joblist->ridx, pmgr->joblistsize, pmgr->resize, pmgr->sleepSum);

    close(mmapFd);
    return PERSISTENCE_RET_INIT_SUCCESS;
}

static int _pushJobList(const char* wbuf, int len)
{
    if (len >= MAX_PERSISTENCE_BUF_SIZE) {
        return PERSISTENCE_RET_SIZE_OVERFLOW;
    }
    while (pmgr->joblist->widx + len + JOBLEN_SIZE > pmgr->joblistsize) { //full
        _resizeJobListStart();
    }
    char* start = pmgr->joblist->buf + pmgr->joblist->widx;


    memcpy(start + JOBLEN_SIZE, wbuf, len);
    memcpy(start, &len, JOBLEN_SIZE);

    redisLog(REDIS_DEBUG, "write joblist widx %d  wbuf %p", pmgr->joblist->widx, start);
    
    _incJoblistWriteidx(len + JOBLEN_SIZE);
    
    return PERSISTENCE_RET_PUSHJOBLIST_SUCCESS;
}

static int _resizeJobListStart(void)
{
    redisLog(REDIS_WARNING, "joblist resize widx %d start", pmgr->joblist->widx);
    pmgr->resize = 1;
    while (pmgr->resize) {
        _waitSleep();
    }
    return PERSISTENCE_RET_RESET_SUCCESS;
}

static int _resizeJobListEnd(void)
{
    int backuplen = sizeof(JobList) + pmgr->joblist->widx;
    char* backupdata = zmalloc(backuplen);
    memcpy(backupdata, pmgr->joblist, backuplen);
    _initJoblist(pmgr->joblistsize * 2);
    memcpy(pmgr->joblist, backupdata, backuplen);

    pmgr->joblist->widx = pmgr->joblist->widx - pmgr->joblist->ridx;
    pmgr->joblist->ridx = 0;
    pmgr->resize = 0;

    zfree(backupdata);
    redisLog(REDIS_WARNING, "joblist resize ridx complete %d", pmgr->joblist->ridx);
    return PERSISTENCE_RET_RESET_SUCCESS;
}

static void _incJoblistWriteidx(int inc)
{
    pmgr->joblist->widx += inc;
}

static void _incJoblistReadidx(int inc)
{
    pmgr->joblist->ridx += inc;
}

static int _popJobList(char** rbuf)
{
    if (pmgr->resize) {
        _resizeJobListEnd();
    }
    if (pmgr->joblist->ridx >= pmgr->joblist->widx) { //empty
        return PERSISTENCE_RET_POPJOBLIST_EMPTY;
    }
    int len = 0;
    char* start = pmgr->joblist->buf + pmgr->joblist->ridx;
    memcpy(&len, start, JOBLEN_SIZE);
    *rbuf = start + JOBLEN_SIZE;
    redisLog(REDIS_DEBUG, "read joblist ridx %d rbuf %p", pmgr->joblist->ridx, start);
    return len;
}

static void* _persistenceMain(void* arg)
{
    while (1) {
        char* recv;
        int len = _popJobList(&recv);
        if (len == PERSISTENCE_RET_POPJOBLIST_EMPTY) {
            _waitSleep();
        } else {
            CmdArgv* cmdArgvs[MAX_CMD_ARGV];
            redisCommandProc* proc;
            int argc = _unpackCmd(recv, len, cmdArgvs, &proc);
            if (argc > 0) {
                writeToDB(argc, cmdArgvs, proc);
            }
            _incJoblistReadidx(len + JOBLEN_SIZE);
        }
    }
    return NULL;
}

int initPersistence(int joblistsize, const char* mmapFile)
{
    pmgr = (PMgr*)zmalloc(sizeof(PMgr));
    pmgr->mmapFile = mmapFile;
    pmgr->resize = 0;
    pmgr->sleepSum = 0;
    int ret = _initJoblist(joblistsize);
    if (ret != PERSISTENCE_RET_INIT_SUCCESS) {
        return ret;
    }
    pthread_t thread;
    pthread_attr_t attr;
    pthread_attr_init(&attr);
    pthread_create(&thread, &attr, _persistenceMain, NULL);
    return PERSISTENCE_RET_INIT_SUCCESS;
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
    return len > 0 ? _pushJobList(wbuf, len) : PERSISTENCE_RET_SIZE_OVERFLOW;
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
            return PERSISTENCE_RET_SIZE_OVERFLOW;
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
    }
    return i;
}

int persistenceUntreatedSize(void)
{
    return pmgr != NULL ? pmgr->joblist->widx - pmgr->joblist->ridx : -1;
}

int persistenceWaitSleepSum(void)
{
    return pmgr != NULL ? pmgr->sleepSum : -1;
}

static void _waitSleep(void)
{
    usleep(1000);
    pmgr->sleepSum += 1;
    if (pmgr->sleepSum >= 2147483640) {
        pmgr->sleepSum = 0;
    }
}
