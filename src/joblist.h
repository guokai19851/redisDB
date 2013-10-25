#define JOBLIST_RET_SIZE_OVERFLOW -1
#define JOBLIST_RET_POP_EMPTY -5
#define JOBLIST_RET_PUSH_SUCCESS 0
#define JOBLIST_RET_SUCCESS 0
#define JOBLEN_SIZE 4

#define NEXT_POT(N) do {\
            (N) -= 1; \
            (N) |= (N) >> 16;\
            (N) |= (N) >> 8;\
            (N) |= (N) >> 4;\
            (N) |= (N) >> 2;\
            (N) |= (N) >> 1;\
            (N) += 1;\
        } while (0)

//单读单写非阻塞队列
typedef struct _JobBuff {
    unsigned long long wSize;
    unsigned long long rSize;
    int dirtysize;
    char buf[];
} JobBuff;

typedef struct _JobList {
    int maxBufSize;
    int listsize;
    int listsizeMask;
    int resize;
    const char* mmapFile;
    JobBuff* jobbuff;
} JobList;

int pushJobList(JobList* this, const char* wbuf, int len);
int popJobList(JobList* this, char** rbuf);
JobList* initJoblist(int maxBufSize, int listsize, const char* mmapFile);
void incJoblistRsize(JobList* this, int inc);
