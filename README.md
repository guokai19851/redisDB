redisDB
=======

基于redis和mysql的数据持久化方案

redis运行时连接数据库 cache失效时同步读mysql, 数据修改时候异步通过DB线程写mysql
目前支持 string, list, zset, 以及incr 格式

