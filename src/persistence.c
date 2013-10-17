#include "mysqlDB.h"
#include "persistence.h"
#include "zmalloc.h"

#include <assert.h>
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
static int _getjoblistWidx(void);
static int _getjoblistRidx(void);
static int _isjoblistempty(void);
static int _isjoblistfull(int pushsize);

//单读单写非阻塞队列
typedef struct _JobList {
    unsigned long long wsize;
    unsigned long long rsize;
    int dirtysize;
    char buf[];
} JobList;

typedef struct _PMgr {
    JobList* joblist;
    int joblistsize;
    int joblistsizeMask;
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
        pmgr->joblist->rsize = pmgr->joblist->wsize = 0;
        pmgr->joblist->dirtysize = 0;
    }
    pmgr->joblistsize = filesize - sizeof(JobList);
    pmgr->joblistsizeMask = pmgr->joblistsize - 1;
    redisLog(REDIS_WARNING, "persistence joblist wsize:%d\t rsize:%d\t len:%d resize:%d \tsleepSum:%d", pmgr->joblist->wsize, pmgr->joblist->rsize, pmgr->joblistsize, pmgr->resize, pmgr->sleepSum);

    close(mmapFd);
    return PERSISTENCE_RET_INIT_SUCCESS;
}

static int _pushJobList(const char* wbuf, int len)
{
    if (len >= MAX_PERSISTENCE_BUF_SIZE) {
        return PERSISTENCE_RET_SIZE_OVERFLOW;
    }
    int widx = _getjoblistWidx();
    assert(pmgr->joblist->rsize <= pmgr->joblist->wsize);
    if (_isjoblistfull(len + JOBLEN_SIZE)) {
        widx = _resizeJobListStart();
    }
    char* start = pmgr->joblist->buf + widx;

    memcpy(start + JOBLEN_SIZE, wbuf, len);
    memcpy(start, &len, JOBLEN_SIZE);

    redisLog(REDIS_DEBUG, "write joblist wsize %d  wbuf %p", pmgr->joblist->wsize, start);
    
    _incJoblistWriteidx(len + JOBLEN_SIZE);
    
    return PERSISTENCE_RET_PUSHJOBLIST_SUCCESS;
}

static int _resizeJobListStart(void)
{
    assert(pmgr->resize == 0);
    redisLog(REDIS_WARNING, "joblist resize start %d, %d", pmgr->joblist->wsize, pmgr->joblist->rsize);
    pmgr->resize = 1;
    while (pmgr->resize) {
        _waitSleep();
    }
    return _getjoblistWidx();
}

static int _resizeJobListEnd(void)
{
    int backuplen = pmgr->joblist->wsize - pmgr->joblist->rsize - pmgr->joblist->dirtysize;
    assert(backuplen <= pmgr->joblistsize);
    int ridx = _getjoblistRidx();
    int widx = _getjoblistWidx();
    assert(widx <= ridx);
    int oldjoblistsize = pmgr->joblistsize;
    char* backup = zmalloc(backuplen);
    char* start = backup;

    int behindUnreadSize = oldjoblistsize - ridx - pmgr->joblist->dirtysize;
    int frontUnreadSize = widx; 
    memcpy(start, pmgr->joblist->buf + ridx, behindUnreadSize);
    start += behindUnreadSize;
    memcpy(start, pmgr->joblist->buf, frontUnreadSize);
    _initJoblist(oldjoblistsize * 2);

    pmgr->joblist->wsize = frontUnreadSize + behindUnreadSize;
    pmgr->joblist->rsize = 0;
    memcpy(pmgr->joblist->buf, backup, backuplen);
    
    zfree(backup);
    redisLog(REDIS_WARNING, "joblist resize ridx complete ok, %d, %d", pmgr->joblist->wsize, pmgr->joblist->rsize);
    
    pmgr->resize = 0;
    return _getjoblistRidx();
}

static void _incJoblistWriteidx(int inc)
{
    pmgr->joblist->wsize += inc;
}

static void _incJoblistReadidx(int inc)
{
    pmgr->joblist->rsize += inc;
}

static int _popJobList(char** rbuf)
{
    int len = 0;
    int ridx = _getjoblistRidx();
    if (pmgr->resize) {
        ridx = _resizeJobListEnd();
    } 
    assert(pmgr->joblist->rsize <= pmgr->joblist->wsize);
    if (_isjoblistempty()) { //empty
        return PERSISTENCE_RET_POPJOBLIST_EMPTY;
    }
    char* start = pmgr->joblist->buf + ridx;
    memcpy(&len, start, JOBLEN_SIZE);
    redisLog(REDIS_DEBUG, "recv len %d", len);
    assert(len > 0 && len <= MAX_PERSISTENCE_BUF_SIZE);
    *rbuf = start + JOBLEN_SIZE;
    redisLog(REDIS_DEBUG, "read joblist rsize %d rbuf %p", pmgr->joblist->rsize, start);
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
        assert(i <= MAX_CMD_ARGV);
    }
    return i;
}

void persistenceInfo(int* untreatedSize, int* sleepSum, unsigned long long * wsize, unsigned long long * rsize)
{
    *untreatedSize = pmgr != NULL ? pmgr->joblist->wsize - pmgr->joblist->rsize : -1;
    *sleepSum = pmgr != NULL ? pmgr->sleepSum : -1;
    *wsize = pmgr->joblist->wsize;
    *rsize = pmgr->joblist->rsize;
}

static void _waitSleep(void)
{
    usleep(1000);
    pmgr->sleepSum += 1;
    if (pmgr->sleepSum >= 2147483640) {
        pmgr->sleepSum = 0;
    }
}

static int _getjoblistWidx(void)
{
    int widx = pmgr->joblist->wsize & pmgr->joblistsizeMask;
    if (widx + MAX_PERSISTENCE_BUF_SIZE > pmgr->joblistsize) {
        pmgr->joblist->dirtysize = pmgr->joblistsize - widx;
        pmgr->joblist->wsize += pmgr->joblistsize - widx; 
        widx = 0;  
    }
    return widx;
}

static int _getjoblistRidx(void)
{
    int ridx = pmgr->joblist->rsize & pmgr->joblistsizeMask;
    if (ridx + MAX_PERSISTENCE_BUF_SIZE > pmgr->joblistsize) {
        assert(pmgr->joblist->dirtysize == pmgr->joblistsize - ridx);
        pmgr->joblist->rsize += pmgr->joblistsize - ridx; 
        ridx = 0;  
    }
    return ridx;
}

static int _isjoblistempty(void)
{
    return pmgr->joblist->rsize == pmgr->joblist->wsize;
}

static int _isjoblistfull(int pushsize)
{
    return pmgr->joblist->wsize + pushsize - pmgr->joblist->rsize > pmgr->joblistsize;
}
