from distutils.version import StrictVersion
import unittest
import datetime
import time
import binascii
from redis._compat import (unichr, u, b, ascii_letters, iteritems, dictkeys, dictvalues)
from redis.client import parse_info
import redis
from multiprocessing import Pool
import MySQLdb as mysql

#conn  =  mysql.connect(host="localhost", user="root", passwd="", db="redisDB")
#cursor   =  conn.cursor(cursorclass = mysql.cursors.DictCursor)
#cursor.execute("set names utf8")
#for i in xrange(0, 5000):
#    cursor.execute("BEGIN")
#    cursor.execute("UPDATE  `teststr` set val = 'aaaaaaaaaaaaaa%s'"% (i));
#    cursor.execute("COMMIT")

def start(args):
    rCnt, name = args
    c = redis.Redis(host = 'localhost', port = 6379, db = 9)
    for i in range(1, rCnt):
#        c.set(name, "aaoo" + str(i));
#        c.lpush(name+"list_" + str(i), "aaaaaa_" + str(i))
#        print c.rpop(name+"list_" + str(i))
#        print c.incr("abc")
        c.zadd(name + "zset", **{'a': i + 1, 'b': i + 2, 'c': i + 3})
        c.zremrangebyrank(name + "zset", 0, 3)
#        c.zrem(name + "zset", 'a')
#        c.zrem(name + "zset", 'b')
#        c.zrem(name + "zset", 'c')

if __name__ == '__main__':
#    start((100, "test"))
    p = Pool(50)
    args = []
    for i in range(0, 50):
        args.append((100, "test"))
    p.map(start, args)
