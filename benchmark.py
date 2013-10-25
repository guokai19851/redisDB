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
        c.set(name, "aaoo" + str(i));
#        c.lpush(name+"list", "aaaaaa_" + str(i))
#        c.incr("abc")
#        c.zadd(name + "zset_" + str(i), **{'a': 1, 'b': 2, 'c': 3})

if __name__ == '__main__':
    p = Pool(50)
    args = []
    for i in range(0, 500):
        args.append((100, "teststr"))
    p.map(start, args)
