# 简介
	binlog_rollback实现了基于row格式binlog的回滚闪回功能，让误删除或者误更新数据，可以不停机不使用备份而快速回滚误操作。
	binlog_rollback也可以解释binlog（支持非row格式binlog）生成易读的SQL，让查找问题如什么时个某个表的某个字段的值被更新成了1，或者找出某个时间内某个表的所有删除操作等问题变得简单。
	binlog_rollback可以按配置输出各个表的update/insert/delete统计报表， 也会输出大事务与长事务的分析， 应用是否干了坏事一目了然， 也会输出所有DDL。
    binlog_rollback通过解释mysql/mariadb binlog/relaylog实现以下三大功能:
        1）flashback/闪回/回滚， DML回滚到任意时间或者位置。
        	生成的文件名为rollback.xxx.sql或者db.tb.rollback.xxx.sql
            生成的SQL形式如下
            ```sql
            begin
            DELETE FROM `danny`.`emp` WHERE `id`=1
            # datetime=2017-10-23_00:14:28 database=danny table=emp binlog=mysql-bin.000012 startpos=417 stoppos=575
            commit
            ```
        2）前滚，把binlog/relaylog的DML解释成易读的SQL语句。
        	*支持非row格式的binlog， 默认不解释非row格式的DML， 需要指定参数-stsql
        	生成的文件名为forward.xxx.sql或者db.tb.forward.xxx.sql
            生成的SQL形式如下
            ```sql
            begin
            # datetime=2017-10-23_00:14:28 database=danny table=emp binlog=mysql-bin.000012 startpos=417 stoppos=575
            INSERT INTO `danny`.`emp` (`id`,`name`,`sr`,`icon`,`points`,`sa`,`sex`) VALUES (1,'张三1','华南理工大学&SCUT',X'89504e47',1.1,1.1,1)
            commit
            ```
        3）统计分析， 输出各个表的DML统计， 输出大事务与长事务， 输出所有的DDL 
        	DML统计结果文件：binlog_stats.txt
			大事务与长事务结果文件：big_long_trx.txt
			DDL结果文件：ddl_info.txt  
![DML统计](https://github.com/GoDannyLai/binlog_rollback/raw/master/misc/img/dml_report.png)
![大事务与长事务](https://github.com/GoDannyLai/binlog_rollback/raw/master/misc/img/long_big_trx.png)
![DDL信息](https://github.com/GoDannyLai/binlog_rollback/raw/master/misc/img/ddl_info.png)
        4) 输出row格式下的原始SQL（5.7）
        	结果文件名为original_sql.binlogxxx.sql
![原始SQL](https://github.com/GoDannyLai/binlog_rollback/raw/master/misc/img/org_sql.png)
       
    *以上功能均可指定任意的单库多库， 单表多表， 任意时间点， 任意binlog位置。
    *支持mysql5.5及以上，也支持mariadb的binlog， 支持传统复制的binlog， 也支持GTID的binlog。
    *支持直接指定文件路径的binlog， 也支持主从复制， binlog_rollback作为从库从主库拉binlog来过解释。
    *也支持目标binlog中包含了DDL(增加与减少表字段， 变化表字位置)的场景。
# 限制
    *使用回滚/闪回功能时，binlog格式必须为row,且binlog_row_image=full， 其它功能支持非row格式binlog
    *只能回滚DML， 不能回滚DDL
    *支持V4格式的binlog， V3格式的没测试过，测试与使用结果显示，mysql5.1，mysql5.5, mysql5.6与mysql5.7的binlog均支持
    *支持指定-tl时区来解释binlog中time/datetime字段的内容。开始时间-sdt与结束时间-edt也会使用此指定的时区， 
      但注意此开始与结束时间针对的是binlog event header中保存的unix timestamp。结果中的额外的datetime时间信息都是binlog event header中的unix timestamp
    *decimal字段使用float64来表示， 但不损失精度
    *所有字符类型字段内容按golang的utf8(相当于mysql的utf8mb4)来表示
# 常用场景    
    1）数据被误操作， 需要把某几个表的数据不停机回滚到某个时间点
    2）数据异常， 帮忙从binlog中找出这个表的某些数据是什么时间修改成某些值的
    3）IO高TPS高， 帮忙查出那些表在频繁更新
    4）需要把这个表从昨晚1点到3点的更新提供给开发查问题
    5）帮忙找出某个时间点数据库是否有大事务或者长事务
# 特点
    1）速度快。 解释512MB的binlog：
        注意CPU的负载， 如果IO不是瓶颈， 会使用满几个核， 请按需调整线程数
        1.1）生成回滚的SQL只需要1分26秒(6线程)
        
![](https://github.com/GoDannyLai/binlog_rollback/raw/master/misc/img/time_rollback.png)

        1.2）生成前滚的SQL只需要1分26秒(6线程)
        
![](https://github.com/GoDannyLai/binlog_rollback/raw/master/misc/img/time_2sql.png)

        1.3）生成表DML统计信息， 大事务与长事务统计信息只需要55秒
        
![](https://github.com/GoDannyLai/binlog_rollback/raw/master/misc/img/time_stats.png)

        1.4）mysqlbinlog解释同样的binlog只需要36秒
        
![](https://github.com/GoDannyLai/binlog_rollback/raw/master/misc/img/time_mysqlbinlog.png)
    
    2) 支持V4版本的binlog， 支持传统与GTID的binlog， 支持mysql5.1与mairiadb5.5及以上版本的binlog， 也同样支持relaylog(结果中注释的信息binlog=xxx startpos=xxx stoppos=xx是对应的主库的binlog信息)
        --mtype=mariadb
    3）支持以时间及位置条件过滤， 并且支持单个以及多个连续binlog的解释。
    	区间范围为左闭右开， [-sxx， -exxx)
        解释binlog的开始位置：
            -sbin mysql-bin.000101
            -spos 4
        解释binlog的结束位置：
            -ebin mysql-bin.000105
            -epos 4
        解释binlog的开始时间    
            -sdt "2018-04-21 00:00:00"
        解释binlog的结束时间  
            -edt "2018-04-22 11:00:00"
    4）支持以库及表条件过滤, 以逗号分隔
    	支持正则表达式，如-dbs "db\d+,db_sh\d+"。正则表达式中请使用小写字母，因为数据库名与表名会先转成小写再与正则表达式进行匹配
        -dbs db1,db2
        -tbs tb1,tb2
    5）支持以DML类型(update,delete,insert)条件过滤
        -sql delete,update
    6) 支持分析本地binlog，也支持复制协议， binlog_rollback作为一个从库从主库拉binlog来本地解释
        -m file //解释本地binlog
        -m repl //binlog_rollback作为slave连接到主库拉binlog来解释
    7）输出的结果支持一个binlog一个文件， 也可以一个表一个文件
        -f 
        例如对于binlog mysql-bin.000101, 如果一个表一个文件， 则生成的文件形式为db.tb.rollback.101.sql(回滚)，db.tb.forward.101.sql(前滚)，
        否则是rollback.101.sql(回滚),forward.101.sql(前滚)
    8）输出的结果是大家常见的易读形式的SQL，支持表名前是否加数据库名
        -d
        ```sql
        begin
        # datetime=2017-10-23_00:14:34 database=danny table=emp binlog=mysql-bin.000012 startpos=21615 stoppos=22822
        UPDATE `danny`.`emp` SET `sa`=1001 WHERE `id`=5;
        # datetime=2017-10-23_00:14:45 database=danny table=emp binlog=mysql-bin.000012 startpos=22822 stoppos=23930
        UPDATE `danny`.`emp` SET `name`=null WHERE `id`=5;
        commit
        ```
        否则为
         ```sql
        begin
        # datetime=2017-10-23_00:14:34 database=danny table=emp binlog=mysql-bin.000012 startpos=21615 stoppos=22822
        UPDATE `emp` SET `sa`=1001 WHERE `id`=5;
        # datetime=2017-10-23_00:14:45 database=danny table=emp binlog=mysql-bin.000012 startpos=22822 stoppos=23930
        UPDATE `emp` SET `name`=null WHERE `id`=5;
        commit
        ```
        
    9）输出结果支持是否保留事务
        -k
        ```sql
        begin
        # datetime=2017-10-23_00:14:34 database=danny table=emp binlog=mysql-bin.000012 startpos=21615 stoppos=22822
        UPDATE `danny`.`emp` SET `sa`=1001 WHERE `id`=5;
        # datetime=2017-10-23_00:14:45 database=danny table=emp binlog=mysql-bin.000012 startpos=22822 stoppos=23930
        UPDATE `danny`.`emp` SET `name`=null WHERE `id`=5;
        commit
        ```
        不保留则是这样：
        ```sql
        # datetime=2017-10-23_00:14:34 database=danny table=emp binlog=mysql-bin.000012 startpos=21615 stoppos=22822
        UPDATE `danny`.`emp` SET `sa`=1001 WHERE `id`=5;
        # datetime=2017-10-23_00:14:45 database=danny table=emp binlog=mysql-bin.000012 startpos=22822 stoppos=23930
        UPDATE `danny`.`emp` SET `name`=null WHERE `id`=5;
        ```
        如果复制因为特别大的事务而中断， 则可以以不保留事务的形式生成前滚的SQL, 在从库上执行， 然后跳过这个事务， 再启动复制， 免去重建从库的
        麻烦， 特别是很大的库
    10）支持输出是否包含时间与binlog位置信息
        -e
        包含额外的信息则为
        ```sql
        # datetime=2017-10-23_00:14:34 database=danny table=emp binlog=mysql-bin.000012 startpos=21615 stoppos=22822
        UPDATE `danny`.`emp` SET `sa`=1001 WHERE `id`=5;
        # datetime=2017-10-23_00:14:45 database=danny table=emp binlog=mysql-bin.000012 startpos=22822 stoppos=23930
        UPDATE `danny`.`emp` SET `name`=null WHERE `id`=5;
        ```
        否则为
        ```sql
        UPDATE `danny`.`emp` SET `sa`=1001 WHERE `id`=5;
        UPDATE `danny`.`emp` SET `name`=null WHERE `id`=5;
        ```
    11）支持生成的SQL只包含最少必须的字段, 前提下是表含有主键或者唯一索引
        默认为
        ```sql
        UPDATE `danny`.`emp` SET `sa`=1001 WHERE `id`=5;
        DELETE FROM `danny` WHERE `id`=5;
        ```
        -a 则为
        ```sql
        UPDATE `danny`.`emp` SET `id`=5, `age`=21, `sex`='M',`sa`=1001, `name`='Danny' WHERE `id`=5 and `age`=21 and `sex`='M' and `sa`=900 and `name`='Danny';
        DELETE FROM `danny` WHERE `id`=5 and `age`=21 and `sex`='M' and `sa`=900 and `name`='Danny';
        ```
    12） 支持优先使用唯一索引而不是主键来构建where条件
        -U
        有时不希望使用主健来构建wheret条件， 如发生双写时， 自增主健冲突了， 这时使用非主健的唯一索引来避免生成的SQL主健冲突
    13） 支持生成的insert语句不包含主健
        -I
        发生双写时， 自增主健冲突了， 这时使用这个参数来让生成的insert语句不包括主健来避免生成的SQL主健冲突
    14）支持大insert拆分成小insert语句。
        -r 100
        对于一个insert 1000行的插入， 会生成10个insert语句，每个语句插入100行
    
    15）支持非row格式binlog的解释
    	当-w 2sql时加上参数-stsql，则会解释非row格式的DML语句。使用的是https://github.com/pingcap/parser的SQL解释器来解释DDL与非row格式的DML。
		由于不是支持所有要SQL， 如create trigger就不支持， 遇到SQL无法解释时会报错退出， 如需要跳过该SQL并继续解释， 请使用参数-ies。-ies 后接正则表达式，
		解释错误或者无法解释的SQL如果匹配-ies指定的正则表达式， 则binlog_rollback不会退出而是跳过该SQL继续解释后面的binlog， 否则错误退出。
		-ies后接的正则表达式请使用小写字母, 因为binlog_rollback会先把SQL转成小写再与之匹配。
     
    16）支持目标binlog中包含DDL(增减字段,变化字段位置)的情形
        binlog只保存了各个字段的位置， 并没有保存各个字段的名字。在前滚与回滚的模式下, binlog_rollback需要拿到表结构信息来生成易读的SQL, 如果表结构有变化, 那如何处理？
        例如表tmp的DDL如下
        ```sql
        create table emp (name varchar(50), sr text, points float, sa decimal(10,3), sex enum("f", "m"), icon blob)
        alter table emp add column id int  first
        truncate table emp
        alter table emp add primary key (id)
        alter table emp modify id int auto_increment
        alter TABLE emp add column updatetime datetime comment '更新时间', add createtime timestamp default current_timestamp comment '创建时间'
        alter TABLE emp drop column updatetime
        ```
        但binlog_rollback这时获取到的表结构表结构如下
        ```sql
        CREATE TABLE `emp` (
          `id` int(11) NOT NULL AUTO_INCREMENT,
          `name` varchar(50) DEFAULT NULL,
          `sr` text,
          `points` float DEFAULT NULL,
          `sa` decimal(10,3) DEFAULT NULL,
          `sex` enum('f','m') DEFAULT NULL,
          `icon` blob,
          `createtime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
          PRIMARY KEY (`id`)
        ) ENGINE=InnoDB AUTO_INCREMENT=1000 DEFAULT CHARSET=utf8   
        ```
       不清楚之前的表结构， 就会出现错乱:
       ```sql
       begin;
       # datetime=2018-02-05_10:12:41 database=danny table=emp binlog=mysql-bin.000001 startpos=1614 stoppos=1772
       INSERT INTO `danny`.`emp` (`id`,`name`,`sr`,`points`,`sa`,`sex`) VALUES ('张三1',X'e58d8ee58d97e79086e5b7a5e5a4a7e5ada62653435554',1.100000023841858,1.1,1,X'89504e47');
       commit;
       ```
       binlog_rollback会输出所有DDL的语句到ddl_info.log这个文件， 有时间与位置信息，如:
       ```sql
        datetime            binlog            startpos   stoppos    sql
        2018-02-05_10:12:18 mysql-bin.000001  1115       1320       create table emp (name varchar(50), sr text, points float, sa decimal(10,3), sex enum("f", "m"), icon blob)
        2018-02-05_10:15:10 mysql-bin.000001  8556       8694       alter table emp add column id int  first
        2018-02-05_10:16:41 mysql-bin.000001  8759       8856       truncate table emp
        2018-02-05_10:16:42 mysql-bin.000001  8921       9055       alter table emp add primary key (id)
        2018-02-05_10:17:21 mysql-bin.000001  9120       9262       alter table emp modify id int auto_increment
        2018-02-05_13:46:18 mysql-bin.000001  400409     400653     alter TABLE emp add column updatetime datetime comment '更新时间', add createtime timestamp default current_timestamp comment '创建时间'
       ```
       表结构信息会dump到-dj指定的文件（默认tblDef.json）， 如：
       ```json
        {
            "danny.emp": {
                "_/0/0": {
                    "database": "danny",
                    "table": "emp",
                    "columns": [
                        {
                            "column_name": "id",
                            "column_type": "int"
                        },
                        {
                            "column_name": "name",
                            "column_type": "varchar"
                        },
                        {
                            "column_name": "sr",
                            "column_type": "text"
                        },
                        {
                            "column_name": "points",
                            "column_type": "float"
                        },
                        {
                            "column_name": "sa",
                            "column_type": "decimal"
                        },
                        {
                            "column_name": "sex",
                            "column_type": "enum"
                        },
                        {
                            "column_name": "icon",
                            "column_type": "blob"
                        },
                        {
                            "column_name": "createtime",
                            "column_type": "timestamp"
                        }
                    ],
                    "primary_key": [
                        "id"
                    ],
                    "unique_keys": [],
                    "ddl_info": {
                        "binlog": "_",
                        "start_position": 0,
                        "stop_position": 0,
                        "ddl_sql": ""
                    }
                }
            }
        }
       ```
       结合上面的信息， 手动修改tblDef.json， 让其也保存有DDL前的表结构：
       ```json
        {
            "danny.emp": {
                "mysql-bin.000001/8556/8694": {
                    "database": "danny",
                    "table": "emp",
                    "columns": [
                        {
                            "column_name": "name",
                            "column_type": "varchar"
                        },
                        {
                            "column_name": "sr",
                            "column_type": "text"
                        },
                        {
                            "column_name": "points",
                            "column_type": "float"
                        },
                        {
                            "column_name": "sa",
                            "column_type": "decimal"
                        },
                        {
                            "column_name": "sex",
                            "column_type": "enum"
                        },
                        {
                            "column_name": "icon",
                            "column_type": "blob"
                        }
                    ],
                    "primary_key": [],
                    "unique_keys": [],
                    "ddl_info": {
                        "binlog": "mysql-bin.000001",
                        "start_position": 8556,
                        "stop_position": 8694,
                        "ddl_sql": ""
                    }
                },
                "_/0/0": {
                    "database": "danny",
                    "table": "emp",
                    "columns": [
                        {
                            "column_name": "id",
                            "column_type": "int"
                        },
                        {
                            "column_name": "name",
                            "column_type": "varchar"
                        },
                        {
                            "column_name": "sr",
                            "column_type": "text"
                        },
                        {
                            "column_name": "points",
                            "column_type": "float"
                        },
                        {
                            "column_name": "sa",
                            "column_type": "decimal"
                        },
                        {
                            "column_name": "sex",
                            "column_type": "enum"
                        },
                        {
                            "column_name": "icon",
                            "column_type": "blob"
                        },
                        {
                            "column_name": "createtime",
                            "column_type": "timestamp"
                        }
                    ],
                    "primary_key": [
                        "id"
                    ],
                    "unique_keys": [],
                    "ddl_info": {
                        "binlog": "_",
                        "start_position": 0,
                        "stop_position": 0,
                        "ddl_sql": ""
                    }
                }
            }
        }
       ```
       并加上参数-rj tblDef.json -dj tblDef_dump.json -oj 让binlog_rollback从tblDef.json获取表结构信息， 重新运行， 生成的SQL无误了
       ```sql
        begin;
        # datetime=2018-02-05_10:12:41 database=danny table=emp binlog=mysql-bin.000001 startpos=1614 stoppos=1772
        INSERT INTO `danny`.`emp` (`name`,`sr`,`points`,`sa`,`sex`,`icon`) VALUES ('张三1','华南理工大学&SCUT',1.100000023841858,1.1,1,X'89504e47');
        commit;
       ```
# 安装与使用
    1)安装
        https://github.com/GoDannyLai/binlog_rollback/releases中有编译好的linux与window二进制版本， 可以直接使用， 无其它依赖。
        如果需要编译， 请使用GO>=1.8.3版本来编译。使用的其中两个依赖库https://github.com/siddontang/go-mysql与https://github.com/dropbox/godropbox/database/sqlbuilder
        有修改小部分的源码， 请使用vendor中包，或者按照 `开源库所做的修改.txt` 中来修改https://github.com/siddontang/go-mysql与https://github.com/dropbox/godropbox/database/sqlbuilder
    2）使用
        *生成前滚SQL与DML报表:
            ./binlog_rollback.exe -m repl -w 2sql -M mysql -t 4 -mid 3331 -H 127.0.0.1 -P 3306 -u xxx -p xxx -dbs db1,db2 -tbs tb1,tb2 -sbin mysql-bin.000556 -spos 107 -ebin mysql-bin.000559 -epos 4 -e -f -r 20 -k -b 100 -l 10 -o /home/apps/tmp -dj tbs_all_def.json
        *生成回滚SQL与DML报表:
            ./binlog_rollback.exe -m file -w rollback -M mysql -t 4 -H 127.0.0.1 -P 3306 -u xxx -p xxx -dbs db1,db2 -tbs tb1,tb2 -tbs tb1,tb2 -sdt "2017-09-28 13:00:00" -edt "2017-09-28 16:00:00" -e -f -r 20 -k -b 100 -l 10  -o /home/apps/tmp -dj tbs_all_def.json mysql-bin.000556
        *只生成DML报表:
            ./binlog_rollback -m file -w stats -M mysql -i 20 -b 100 -l 10 -o /home/apps/tmp mysql-bin.000556
# 联系
    已经在生产使用超过一年, 多次在线回滚过数据， 校对数据更新。有任何的bug或者使用反馈， 欢迎联系laijunshou@gmail.com.
