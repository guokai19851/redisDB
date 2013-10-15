#ifndef __MYSQLDB_H__
#define __MYSQLDB_H__

#include "redis.h"
#define MAX_KEY_LEN 32
#define DB_RET_TABLE_NOTEXIST 1146
#define MAX_PERSISTENCE_BUF_SIZE 10240 
#define DB_RET_NOTRESULT -1
#define DB_RET_SUCCESS 0
#define DB_RET_CONNERROR -2
#define DB_RET_DBINITERROR -3
#define DB_RET_KEY_TOO_MANY -4
#define DB_RET_CMD_NOT_FOUND -5
#define DB_RET_EXPIRE -6
#define DB_RET_LIST_NOT_WHERE -7
#define DB_RET_NOT_SUPPORT -8

typedef struct _CmdArgv
{
    int len;
    char buf[];
} CmdArgv;

int readFromDB(redisClient* c);
int writeToDB(int argc, CmdArgv** cmdArgvs, redisCommandProc* proc);
int initDB(const char* host, const int port, const char* user, const char* pwd, const char* dbName);
int isDBError(int ret);

#endif
