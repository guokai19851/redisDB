#include "mysqlDB.h"
#include "dict.h"

#include <stdlib.h>
#include <stdio.h>
#include <time.h>
#include <assert.h>

DBConn* readConn;

static int _query(const char* sql, MYSQL* conn);
static int _parseKey(const char* key, const int keyLen, char* table, char* ID);
static int _begin(DBConn* dbConn);
static int _commit(DBConn* dbConn);
static int _rollback(DBConn* dbConn);
static char* _strmov(char* dest, char* src);
static int _connDB(MYSQL* conn, const char* host, const int port, const char* user, const char* pwd, const char* dbName);
static int _pingDB(MYSQL* conn);

/* 同步读 */
static int _selectStrFromDB(redisClient* c);
static int _loadListFromDB(redisClient* c);
static int _loadZsetFromDB(redisClient* c);
static int _loadIncrFromDB(redisClient* c);
static int _clearExpireStrToDB(const char* table, const char* ID);

/* 异步写 */
static int _popListToDB(CmdArgv* key, int where, DBConn* dbConn);
static int _pushListToDB(CmdArgv* key, CmdArgv* val, int where, int createNotExist, DBConn* dbConn);
static int _writeStrToDB(CmdArgv* key, CmdArgv* val, DBConn* dbConn);
static int _expireat(CmdArgv* key, CmdArgv* expireat, DBConn* dbConn);
static int _createStrTable(const char* table, DBConn* dbConn);
static int _createListTable(const char* table, DBConn* dbConn);
static int _createZsetTable(const char* table, DBConn* dbConn);
static int _createIncrTable(DBConn* dbConn);
static int _zaddToDB(CmdArgv* key, CmdArgv* score, CmdArgv* member, int incr, DBConn* dbConn);
static int _incrToDB(CmdArgv* key, CmdArgv* incr, DBConn* dbConn);
static int _zremrangeToDB(CmdArgv* key, CmdArgv* start, CmdArgv* stop, int rankOrScore, DBConn* dbConn);
static int _zremToDB(CmdArgv* key, CmdArgv* member, DBConn* dbConn);

int initReadDB(const char* host, const int port, const char* user, const char* pwd, const char* dbName)
{
    readConn = initDB(host, port, user, pwd, dbName);
    return readConn != NULL ? DB_RET_SUCCESS : DB_RET_DBINITERROR;
}

DBConn* initDB(const char* host, const int port, const char* user, const char* pwd, const char* dbName)
{
    DBConn* dbConn = (DBConn*)zmalloc(sizeof(DBConn));
    dbConn->conn = mysql_init(NULL);
    if (!dbConn->conn || _connDB(dbConn->conn, host, port, user, pwd, dbName) != DB_RET_SUCCESS) {
        zfree(dbConn);
        return NULL;
    }
    dbConn->sqlbuff = (char*)zmalloc(MAX_SQL_BUF_SIZE * 2);
    return dbConn;
}

static int _begin(DBConn* dbConn)
{
    return _query("BEGIN", dbConn->conn);
}

static int _commit(DBConn* dbConn)
{
    return _query("COMMIT", dbConn->conn);
}

static int _rollback(DBConn* dbConn)
{
    int ret = _query("ROLLBACK", dbConn->conn);
    return ret;
}

static int _query(const char* sql, MYSQL* conn)
{
    redisLog(REDIS_DEBUG, "%s", sql);
    mysql_query(conn, sql);
    int err = mysql_errno(conn);
    if (err > 0) {
        if (err != DB_RET_TABLE_NOTEXIST) {
            redisLog(REDIS_WARNING, "%d, %s, %s", err, sql, mysql_error(conn));
        }
        return err;
    }
    int warningCnt = mysql_warning_count(conn);
    if (warningCnt > 0) {
        redisLog(REDIS_WARNING, "sql warning count %d, %s", warningCnt, sql);
        return warningCnt;
    }
    return DB_RET_SUCCESS;
}

static int _parseKey(const char* key, int keyLen, char* table, char* ID)
{
    int n = 0;
    while (*(key + n) != '_' && n < keyLen) {
        *(table + n) = *(key + n);
        n++;
    }
    n++;
    int j = 0;
    while (*(key + n) != '_' && n < keyLen) {
        *(ID + j++) = *(key + n);
        n++;
    }
    if (j == 0) {
        *ID = '0';
    }
    return j;
}

int writeToDB(int argc, CmdArgv** cmdArgvs, redisCommandProc* proc, DBConn* dbConn)
{
    _pingDB(dbConn->conn);
    int ret = 0;
    int i = 0;
    _begin(dbConn);
    if (proc == setCommand && argc == 2) {
        ret = _writeStrToDB(cmdArgvs[0], cmdArgvs[1], dbConn);

    } else if (proc == msetCommand) {
        for (i = 0; i < argc; i += 2) {
            ret = _writeStrToDB(cmdArgvs[i], cmdArgvs[i + 1], dbConn);
            if (ret != 0) {
                break;
            }
        }

    } else if (proc == expireatCommand) {
        ret = _expireat(cmdArgvs[0], cmdArgvs[1], dbConn);

    } else if (proc == lpushCommand || proc == rpushCommand || proc == lpushxCommand || proc == rpushxCommand) {
        int where = (proc == lpushCommand || proc == lpushxCommand) ? REDIS_HEAD : REDIS_TAIL;
        int createNotExist = (proc == lpushCommand || proc == rpushCommand) ? 1 : 0;
        for (i = 1; i < argc; i++) {
            ret = _pushListToDB(cmdArgvs[0], cmdArgvs[i], where, createNotExist, dbConn);
            if (ret != 0) {
                break;
            }
        }

    } else if ((proc == lpopCommand || proc == rpopCommand) && argc == 1) {
        int where = proc == lpopCommand ? REDIS_HEAD : REDIS_TAIL;
        ret = _popListToDB(cmdArgvs[0], where, dbConn);

    } else if (proc == zaddCommand || proc == zincrbyCommand) {
        int incr = proc == zaddCommand ? 0 : 1;
        for (i = 1; i < argc; i += 2) {
            ret = _zaddToDB(cmdArgvs[0], cmdArgvs[i], cmdArgvs[i + 1], incr, dbConn);
            if (ret != 0) {
                break;
            }
        }

    } else if (proc == zremrangebyscoreCommand || proc == zremrangebyrankCommand) {
        int rankOrScore = proc == zremrangebyrankCommand ? 1 : 0;
        ret = _zremrangeToDB(cmdArgvs[0], cmdArgvs[1], cmdArgvs[2], rankOrScore, dbConn);

    } else if (proc == incrCommand && argc == 1) {
        ret = _incrToDB(cmdArgvs[0], NULL, dbConn);

    } else if (proc == incrbyCommand && argc == 2) {
        ret = _incrToDB(cmdArgvs[0], cmdArgvs[1], dbConn);

    } else if (proc == zremCommand) {
        for (i = 1; i < argc; i++) {
            ret = _zremToDB(cmdArgvs[0], cmdArgvs[i], dbConn);
            if (ret != 0) {
                break;
            }
        }
    } else {
        ret = -1;
    }

    if (ret != 0) {
        _rollback(dbConn);
        return ret;
    }
    _commit(dbConn);
    return DB_RET_SUCCESS;
}

int readFromDB(redisClient* c)
{
    _pingDB(readConn->conn);
    int keylen = strlen(c->argv[1]->ptr);
    if (keylen >= MAX_KEY_LEN) {
        return DB_RET_KEY_TOO_MANY;
    }
    if (c->cmd->proc == getCommand) {
        return _selectStrFromDB(c);

    } else if (c->cmd->proc == lpopCommand
               || c->cmd->proc == rpopCommand
               || c->cmd->proc == lpushCommand
               || c->cmd->proc == rpushCommand
               || c->cmd->proc == lrangeCommand
               || c->cmd->proc == blpopCommand
               || c->cmd->proc == brpopCommand
               || c->cmd->proc == rpoplpushCommand
               || c->cmd->proc == brpoplpushCommand
               || c->cmd->proc == lpushxCommand
               || c->cmd->proc == rpushxCommand
               || c->cmd->proc == lremCommand
               || c->cmd->proc == lsetCommand
              ) {
        return _loadListFromDB(c);

    } else if (c->cmd->proc ==  zrangeCommand
               || c->cmd->proc == zrangebyscoreCommand
               || c->cmd->proc == zaddCommand
               || c->cmd->proc == zcountCommand
               || c->cmd->proc == zincrbyCommand
               || c->cmd->proc == zrankCommand
               || c->cmd->proc == zremCommand
               || c->cmd->proc == zremrangebyscoreCommand
               || c->cmd->proc == zremrangebyrankCommand
               || c->cmd->proc == zrevrangebyscoreCommand
               || c->cmd->proc == zrevrangeCommand
               || c->cmd->proc == zrevrankCommand
               || c->cmd->proc == zscoreCommand
              ) {
        return _loadZsetFromDB(c);

    } else if (c->cmd->proc == incrCommand
               || c->cmd->proc == incrbyCommand
              ) {
        return _loadIncrFromDB(c);

    } else {
        return DB_RET_CMD_NOT_FOUND;
    }
}

static char* _strmov(char* dest, char* src)
{
    while ((*dest++ = *src++));
    return dest - 1;
}

static int _selectStrFromDB(redisClient* c)
{
    MYSQL* conn = readConn->conn;
    char* key = c->argv[1]->ptr;
    char table[16] = {'\0'};
    char ID[16] = {'\0'};
    _parseKey(key, strlen(key), table, ID);
    char* sql = readConn->sqlbuff;
    char* end = _strmov(sql, "SELECT `val`, `expireat` FROM `");
    end += mysql_real_escape_string(conn, end, table, strlen(table));
    end = _strmov(end, "` WHERE `ID` = '");
    end += mysql_real_escape_string(conn, end, ID, strlen(ID));
    end = _strmov(end, "' LIMIT 1");

    *end++ = '\0';
    int ret =  _query(sql, conn);
    MYSQL_RES* res;
    if ((ret == DB_RET_SUCCESS) && (res = mysql_store_result(conn))) {
        if (mysql_num_rows(res) == 0) {
            mysql_free_result(res);
            return DB_RET_NOTRESULT;
        }
        MYSQL_ROW row = mysql_fetch_row(res);
        int expireat = atoi(row[1]);
        int now = (int)time(NULL);
        if (expireat != 0 && now > expireat) {
            _clearExpireStrToDB(table, ID);
            mysql_free_result(res);
            return DB_RET_EXPIRE;
        }
        robj* val = createStringObject(row[0], strlen(row[0]));
        setKey(c->db, c->argv[1], val);
        if (expireat) {
            setExpire(c->db, c->argv[1], expireat * 1000);
        }
        mysql_free_result(res);
        return DB_RET_SUCCESS;
    } else {
        return ret;
    }
}

static int _writeStrToDB(CmdArgv* key, CmdArgv* val, DBConn* dbConn)
{
    MYSQL* conn = dbConn->conn;
    char table[16] = {'\0'};
    char ID[16] = {'\0'};
    _parseKey(key->buf, key->len, table, ID);

    char* sql = dbConn->sqlbuff;
    char* end = _strmov(sql, "INSERT INTO `");
    end += mysql_real_escape_string(conn, end, table, strlen(table));
    end = _strmov(end, "` set `ID` = '");
    end += mysql_real_escape_string(conn, end, ID, strlen(ID));
    end = _strmov(end, "' , `val` = '");
    end += mysql_real_escape_string(conn, end, val->buf, val->len);
    end = _strmov(end, "' ON DUPLICATE KEY UPDATE `val` = '");
    end += mysql_real_escape_string(conn, end, val->buf, val->len);
    *end++ = '\''; *end++ = '\0';
    int ret = _query(sql, conn);
    if (ret == DB_RET_TABLE_NOTEXIST) {
        _createStrTable(table, dbConn);
        return _writeStrToDB(key, val, dbConn);
    }
    return ret;
}

static int _expireat(CmdArgv* key, CmdArgv* expireat, DBConn* dbConn)
{
    MYSQL* conn = dbConn->conn;
    char table[16] = {'\0'};
    char ID[16] = {'\0'};
    _parseKey(key->buf, key->len, table, ID);

    char* sql = dbConn->sqlbuff;
    char* end = _strmov(sql, "UPDATE `");
    end += mysql_real_escape_string(conn, end, table, strlen(table));
    end = _strmov(end, "` set `expireat` = '");
    end += mysql_real_escape_string(conn, end, expireat->buf, expireat->len);
    end = _strmov(end, "' WHERE `ID` = '");
    end += mysql_real_escape_string(conn, end, ID, strlen(ID));
    end = _strmov(end, "' LIMIT 1");

    *end++ = '\0';
    return _query(sql, conn);
}


static int _clearExpireStrToDB(const char* table, const char* ID)
{
    MYSQL* conn = readConn->conn;
    char* sql = readConn->sqlbuff;
    char* end = _strmov(sql, "DELETE FROM `");
    end += mysql_real_escape_string(conn, end, table, strlen(table));
    end = _strmov(end, "` WHERE `ID` = '");
    end += mysql_real_escape_string(conn, end, ID, strlen(ID));
    end = _strmov(end, "' LIMIT 1");

    *end++ = '\0';
    return _query(sql, conn);
}

static int _pushListToDB(CmdArgv* key, CmdArgv* val, int where, int createNotExist, DBConn* dbConn)
{
    MYSQL* conn = dbConn->conn;
    char table[16] = {'\0'};
    char ID[16] = {'\0'};
    char order[16] = {'\0'};
    _parseKey(key->buf, key->len, table, ID);

    char* sql = dbConn->sqlbuff;
    char* end = _strmov(sql, "SELECT ");
    if (where == REDIS_HEAD) {
        end = _strmov(end, " (MIN(`order`) - 1) FROM `");
    } else if (where == REDIS_TAIL) {
        end = _strmov(end, " (MAX(`order`) + 1) FROM `");
    } else {
        return DB_RET_LIST_NOT_WHERE;
    }
    end += mysql_real_escape_string(conn, end, table, strlen(table));
    *end++ = '`'; *end++ = '\0';
    int ret = _query(sql, conn);
    if (ret == DB_RET_TABLE_NOTEXIST) {
        if (createNotExist) {
            _createListTable(table, dbConn);
            return _pushListToDB(key, val, where, createNotExist, dbConn);
        } else {
            return ret;
        }
    }
    MYSQL_RES* res;
    if ((ret == DB_RET_SUCCESS) && (res = mysql_store_result(conn))) {
        if (mysql_num_rows(res) == 0) {
            order[0] = '0'; 
        } else {
            MYSQL_ROW row = mysql_fetch_row(res);
            memcpy(order, row[0], strlen(row[0])); 
        }
        mysql_free_result(res);
    } else {
        return ret;
    }
    
    end = _strmov(sql, "INSERT INTO `");
    end += mysql_real_escape_string(conn, end, table, strlen(table));
    end = _strmov(end, "` SET `order` = '");
    end += mysql_real_escape_string(conn, end, order, strlen(order));
    end = _strmov(end, "' , `ID` = '");
    end += mysql_real_escape_string(conn, end, ID, strlen(ID));
    end = _strmov(end, "' , `val` = '");
    end += mysql_real_escape_string(conn, end, val->buf, val->len);
    *end++ = '\''; *end++ = '\0';

    ret = _query(sql, conn);
    assert(ret != DB_RET_TABLE_NOTEXIST);
    return ret;
}

static int _popListToDB(CmdArgv* key, int where, DBConn* dbConn)
{
    MYSQL* conn = dbConn->conn;
    char table[16] = {'\0'};
    char ID[16] = {'\0'};
    _parseKey(key->buf, key->len, table, ID);
    char* sql = dbConn->sqlbuff;
    char* end = _strmov(sql, "DELETE FROM `");
    end += mysql_real_escape_string(conn, end, table, strlen(table));
    end = _strmov(end, "` WHERE `ID` = '");
    end += mysql_real_escape_string(conn, end, ID, strlen(ID));
    end = _strmov(end, "' ORDER BY `order` ");
    if (where == REDIS_HEAD) {
        end = _strmov(end, " ASC LIMIT 1");
    } else if (where == REDIS_TAIL) {
        end = _strmov(end, " DESC LIMIT 1");
    } else {
        return DB_RET_LIST_NOT_WHERE;
    }
    *end++ = '\0';
    return _query(sql, conn);
}


static int _loadListFromDB(redisClient* c)
{
    MYSQL* conn = readConn->conn;
    char* key = c->argv[1]->ptr;
    char table[16] = {'\0'};
    char ID[16] = {'\0'};
    _parseKey(key, strlen(key), table, ID);
    char* sql = readConn->sqlbuff;
    char* end = _strmov(sql, "SELECT `val` FROM `");
    end += mysql_real_escape_string(conn, end, table, strlen(table));
    end = _strmov(end, "` WHERE `ID` = '");
    end += mysql_real_escape_string(conn, end, ID, strlen(ID));
    end = _strmov(end, "' ORDER BY `order` ASC ");
    *end++ = '\0';

    int ret =  _query(sql, conn);
    MYSQL_RES* res;
    if ((ret == DB_RET_SUCCESS) && (res = mysql_store_result(conn))) {
        int num = mysql_num_rows(res);
        if (num == 0) {
            mysql_free_result(res);
            return DB_RET_NOTRESULT;
        }
        int i = 0;
        robj* lobj = createZiplistObject();
        dbAdd(c->db, c->argv[1], lobj);
        for (; i < num; i++) {
            MYSQL_ROW row = mysql_fetch_row(res);
            robj* val = tryObjectEncoding(createStringObject(row[0], strlen(row[0])));
            listTypePush(lobj, val, REDIS_TAIL);
        }
        mysql_free_result(res);
        return DB_RET_SUCCESS;
    } else {
        return ret;
    }
}

static int _loadZsetFromDB(redisClient* c)
{
    MYSQL* conn = readConn->conn;
    char* key = c->argv[1]->ptr;
    char table[16] = {'\0'};
    char ID[16] = {'\0'};
    _parseKey(key, strlen(key), table, ID);
    char* sql = readConn->sqlbuff;
    char* end = _strmov(sql, "SELECT `member`, `score` FROM `");
    end += mysql_real_escape_string(conn, end, table, strlen(table));
    end = _strmov(end, "` WHERE `ID` = '");
    end += mysql_real_escape_string(conn, end, ID, strlen(ID));
    end = _strmov(end, "'");
    *end++ = '\0';

    int ret =  _query(sql, conn);
    MYSQL_RES* res;
    if ((ret == DB_RET_SUCCESS) && (res = mysql_store_result(conn))) {
        int num = mysql_num_rows(res);
        if (num == 0) {
            mysql_free_result(res);
            return DB_RET_NOTRESULT;
        }
        int i = 0;
        robj* zobj;
        if (server.zset_max_ziplist_entries == 0 ||
            server.zset_max_ziplist_value < sdslen(c->argv[3]->ptr)) {
            zobj = createZsetObject();
        } else {
            zobj = createZsetZiplistObject();
        }
        dbAdd(c->db, c->argv[1], zobj);

        for (; i < num; i++) {
            MYSQL_ROW row = mysql_fetch_row(res);
            robj* member = tryObjectEncoding(createStringObject(row[0], strlen(row[0])));
            float score = atoi(row[1]);
            if (zobj->encoding == REDIS_ENCODING_ZIPLIST) {
                zobj->ptr = zzlInsert(zobj->ptr, member, score);
            } else if (zobj->encoding == REDIS_ENCODING_SKIPLIST) {
                zset* zs = zobj->ptr;
                zskiplistNode* znode  = zslInsert(zs->zsl, score, member);
                incrRefCount(member);
                redisAssertWithInfo(c, NULL, dictAdd(zs->dict, member, &znode->score) == DICT_OK);
                incrRefCount(member);
            }
        }
        mysql_free_result(res);
        return DB_RET_SUCCESS;
    } else {
        return ret;
    }
}

static int _zaddToDB(CmdArgv* key, CmdArgv* score, CmdArgv* member, int incr, DBConn* dbConn)
{
    MYSQL* conn = dbConn->conn;
    char table[16] = {'\0'};
    char ID[16] = {'\0'};
    _parseKey(key->buf, key->len, table, ID);

    char* sql = dbConn->sqlbuff;
    char* end = _strmov(sql, "INSERT INTO `");
    end += mysql_real_escape_string(conn, end, table, strlen(table));
    end = _strmov(end, "` set `ID` = '");
    end += mysql_real_escape_string(conn, end, ID, strlen(ID));
    end = _strmov(end, "' , `member` = '");
    end += mysql_real_escape_string(conn, end, member->buf, member->len);
    end = _strmov(end, "' , `score` = '");
    end += mysql_real_escape_string(conn, end, score->buf, score->len);
    if (incr) {
        end = _strmov(end, "' ON DUPLICATE KEY UPDATE `score` = `score` + '");
    } else {
        end = _strmov(end, "' ON DUPLICATE KEY UPDATE `score` = '");
    }
    end += mysql_real_escape_string(conn, end, score->buf, score->len);
    *end++ = '\''; *end++ = '\0';

    int ret = _query(sql, conn);
    if (ret == DB_RET_TABLE_NOTEXIST) {
        _createZsetTable(table, dbConn);
        return _zaddToDB(key, score, member, incr, dbConn);
    }
    return ret;
}

static int _loadIncrFromDB(redisClient* c)
{
    MYSQL* conn = readConn->conn;
    char* key = c->argv[1]->ptr;
    char* sql = readConn->sqlbuff;
    char* end = _strmov(sql, "SELECT `incr` FROM `INCR_TAB` WHERE `key` = '");
    end += mysql_real_escape_string(conn, end, key, strlen(key));
    end = _strmov(end, "' LIMIT 1");
    *end++ = '\0';

    int ret =  _query(sql, conn);
    MYSQL_RES* res;

    if ((ret == DB_RET_SUCCESS) && (res = mysql_store_result(conn))) {
        if (mysql_num_rows(res) == 0) {
            mysql_free_result(res);
            return DB_RET_NOTRESULT;
        }
        MYSQL_ROW row = mysql_fetch_row(res);
        long long incr = atoll(row[0]);
        robj* new = createStringObjectFromLongLong(incr);
        dbAdd(c->db, c->argv[1], new);
        mysql_free_result(res);
        return DB_RET_SUCCESS;
    } else {
        return ret;
    }
}

static int _incrToDB(CmdArgv* key, CmdArgv* incr, DBConn* dbConn)
{
    MYSQL* conn = dbConn->conn;
    char incrStr[16] = {'\0'};
    if (incr == NULL) {
        incrStr[0] = '1';
    } else {
        memcpy(incrStr, incr->buf, incr->len);
    }
    char* sql = dbConn->sqlbuff;
    char* end = _strmov(sql, "INSERT INTO INCR_TAB set `key` = '");
    end += mysql_real_escape_string(conn, end, key->buf, key->len);
    end = _strmov(end, "', `incr` = '");
    end += mysql_real_escape_string(conn, end, incrStr, strlen(incrStr));
    end = _strmov(end, "' ON DUPLICATE KEY UPDATE `incr` = `incr` + '");
    end += mysql_real_escape_string(conn, end, incrStr, strlen(incrStr));
    *end++ = '\''; *end++ = '\0';
    int ret = _query(sql, conn);
    if (ret == DB_RET_TABLE_NOTEXIST) {
        _createIncrTable(dbConn);
        return _incrToDB(key, incr, dbConn);
    }
    return ret;
}


static int _zremrangeToDB(CmdArgv* key, CmdArgv* start, CmdArgv* stop, int rankOrScore, DBConn* dbConn)
{
    MYSQL* conn = dbConn->conn;
    char table[16] = {'\0'};
    char ID[16] = {'\0'};
    _parseKey(key->buf, key->len, table, ID);

    char* sql = dbConn->sqlbuff;
    char* end = _strmov(sql, "DELETE FROM `");
    end += mysql_real_escape_string(conn, end, table, strlen(table));
    end = _strmov(end, "` WHERE `ID` = '");
    end += mysql_real_escape_string(conn, end, ID, strlen(ID));

    if (rankOrScore) {
        end = _strmov(end, "' AND `member` IN (");
        int limitNum = atoi(stop->buf) - atoi(start->buf) + 1;
        if (limitNum <= 0) {//暂不支持zremrangebyrank tzset 0 -1 这种形式
            return DB_RET_NOT_SUPPORT;
        }
        char limit[8] = {'\0'};
        sprintf(limit, "%d", limitNum);
        
        char sql_2[MAX_SQL_BUF_SIZE] = {'\0'};
        char* end_2 = _strmov(sql_2, "SELECT `member` FROM `");
        end_2 += mysql_real_escape_string(conn, end_2, table, strlen(table));
        end_2 = _strmov(end_2, "` WHERE ID = '");
        end_2 += mysql_real_escape_string(conn, end_2, ID, strlen(ID));
        end_2 = _strmov(end_2, "' ORDER BY `score` ASC LIMIT ");
        end_2 += mysql_real_escape_string(conn, end_2, start->buf, start->len);
        end_2 = _strmov(end_2, ", ");
        end_2 += mysql_real_escape_string(conn, end_2, limit, strlen(limit));
        
        MYSQL_RES* res;
        int ret =  _query(sql_2, conn);
        if ((ret == DB_RET_SUCCESS) && (res = mysql_store_result(conn))) {
            int num = mysql_num_rows(res);
            if (num == 0) {
                mysql_free_result(res);
                return DB_RET_NOTRESULT;
            }
            int i = 0;
            for (; i < num; i++) {
                MYSQL_ROW row = mysql_fetch_row(res);
                *end++ = '\''; end = _strmov(end, row[0]); *end++ = '\'';
                if (i < num - 1) {
                    *end++ = ',';
                }
            }
            end = _strmov(end, ")");
        }
    } else {
        end = _strmov(end, "' AND `score` >= ");
        end += mysql_real_escape_string(conn, end, start->buf, start->len);
        end = _strmov(end, " AND `score` <= ");
        end += mysql_real_escape_string(conn, end, stop->buf, stop->len);
    }
    *end++ = '\0';
    return _query(sql, conn);
}

static int _createStrTable(const char* table, DBConn* dbConn)
{
    MYSQL* conn = dbConn->conn;
    char* sql = dbConn->sqlbuff;
    char* end = _strmov(sql, "CREATE TABLE `");
    end += mysql_real_escape_string(conn, end, table, strlen(table));
    end = _strmov(end, "`( `ID` int(10) NOT NULL DEFAULT 0, `val` BLOB NOT NULL, `expireat` int(10) NOT NULL DEFAULT 0, UNIQUE KEY `IDidx` (`ID`) ) ENGINE=InnoDB DEFAULT CHARSET=utf8 ");
    *end++ = '\0';
    return _query(sql, conn);
}

static int _createListTable(const char* table, DBConn* dbConn)
{
    MYSQL* conn = dbConn->conn;
    char* sql = dbConn->sqlbuff;
    char* end = _strmov(sql, "CREATE TABLE `");
    end += mysql_real_escape_string(conn, end, table, strlen(table));
    end = _strmov(end, "`(`ID` int(10) NOT NULL DEFAULT 0, `order` int(10) NOT NULL DEFAULT 0, `val` BLOB NOT NULL, INDEX `IDidx` (`ID`), INDEX orderidx (`order`) ) ENGINE=InnoDB DEFAULT CHARSET=utf8 ");
    *end++ = '\0';
    return _query(sql, conn);
}

static int _createZsetTable(const char* table, DBConn* dbConn)
{
    MYSQL* conn = dbConn->conn;
    char* sql = dbConn->sqlbuff;
    char* end = _strmov(sql, "CREATE TABLE `");
    end += mysql_real_escape_string(conn, end, table, strlen(table));
    end = _strmov(end, "`(`ID` int(10) NOT NULL DEFAULT 0, `score` int(10) NOT NULL DEFAULT 0, `member` varchar(64) NOT NULL DEFAULT '', INDEX scoreidx (`score`), UNIQUE KEY `memberidx` (`ID`, `member`) ) ENGINE=InnoDB DEFAULT CHARSET=utf8 ");
    *end++ = '\0';
    return _query(sql, conn);
}

static int _createIncrTable(DBConn* dbConn)
{
    MYSQL* conn = dbConn->conn;
    return _query("CREATE TABLE `INCR_TAB` (`key` char(32) NOT NULL DEFAULT '', `incr` int(10) NOT NULL DEFAULT 0, UNIQUE INDEX `keyidx` (`key`)) ENGINE=InnoDB DEFAULT CHARSET=utf8 ", conn);
}

static int _zremToDB(CmdArgv* key, CmdArgv* member, DBConn* dbConn)
{
    MYSQL* conn = dbConn->conn;
    char table[16] = {'\0'};
    char ID[16] = {'\0'};
    _parseKey(key->buf, key->len, table, ID);

    char* sql = dbConn->sqlbuff;
    char* end = _strmov(sql, "DELETE FROM `");
    end += mysql_real_escape_string(conn, end, table, strlen(table));
    end = _strmov(end, "` WHERE `ID` = '");
    end += mysql_real_escape_string(conn, end, ID, strlen(ID));
    end = _strmov(end, "' AND `member` = '");
    end += mysql_real_escape_string(conn, end, member->buf, member->len);
    end = _strmov(end, "' LIMIT 1");

    *end++ = '\0';
    return _query(sql, conn);
}

static int _connDB(MYSQL* conn, const char* host, const int port, const char* user, const char* pwd, const char* dbName)
{
    if (!mysql_real_connect(conn, host, user, pwd, dbName, port, NULL, 0)) {
        redisLog(REDIS_WARNING, "mysql connect error  %d, %s", conn, mysql_error(conn));
        return DB_RET_CONNERROR;
    }
    return DB_RET_SUCCESS;
}

static int _pingDB(MYSQL* conn)
{
    while (mysql_ping(conn)) {
        redisLog(REDIS_WARNING, "mysql connect lost %d, %s", conn, mysql_error(conn));
    }
    return DB_RET_SUCCESS;
}

int isDBError(int ret)
{
    return ret != DB_RET_TABLE_NOTEXIST && ret != DB_RET_NOTRESULT && ret != DB_RET_EXPIRE;
}
