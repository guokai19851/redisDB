redisDB
=======

基于redis和mysql的数据持久化方案

redis运行时连接数据库 cache失效时同步读mysql, 数据修改时候通过消息队列通知DB线程写mysql
reids.conf增加几个配置选项
mysql_host  
mysql_port 
mysql_user 
mysql_pwd
mysql_dbname
persistence_mmap_file:  消息列队指定的mmap映射文件  
write_thread_num 写DB线程数 

对key的命名有规范 "tablename_ID(int)"形式, 如果仅仅是 "tablename" 则系统解析的时候ID默认为0
例如 “user_1” 系统会自动对应"user"表的ID为1的行
     “user_0” 或者 "user" 系统会自动对应"user"表的ID为0的行
    
目前支持 string, list, zset, 以及incr 格式, mysql表结构不需要自己定义，系统自动映射
消息队列采用无锁队列, 支持mmap与malloc两种方式, 采用mmap方式理论上在程序意外死掉的时候不丢失队列数据
经过压力测试, 修改前和修改后的redis性能损耗为10% - 20%, 后期会考虑再进行优化
另外因为是同步读DB, 如果大量的cache失效会带来严重的io阻塞, 从而影响性能, 后期考虑加入配置选项在redis启动以后主动从db恢复数据

基于redis 2.6.16修改
