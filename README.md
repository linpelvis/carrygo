carrygo
===========

[![Build Status](https://travis-ci.org/linpelvis/carrygo.svg?branch=master)](https://travis-ci.org/linpelvis/carrygo)
[![Code Health](https://landscape.io/github/linpelvis/carrygo/master/landscape.svg?style=flat)](https://landscape.io/github/linpelvis/carrygo/master)
[![Requirements Status](https://requires.io/github/linpelvis/carrygo/requirements.svg?branch=master)](https://requires.io/github/linpelvis/carrygo/requirements/?branch=master)


主要作用是同步mysql数据库至hbase，kudu等存储系统。



# 功能 #

- canal日志解析，kafka持久化消息
- 消费kafka消息同步至hbase等存储引擎
- mysql的数据同步支持DDL、DML至hbase


# 原理 #

carrygo解析及存储服务主要有两个模块，分别是parser和store。

### CarryServer ###
1. 基于canal解析mysql的binlog，批量获取message的entry
2. 过滤出需同步的db，并解析数据流，同步到kafka中

### WriterServer ###
1. 基于topic接受kafka的消息
2. HBaseWriter解析数据并存储

# 相关资源 #
canal：<a href="https://github.com/alibaba/canal">https://github.com/alibaba/canal</a>


# 问题反馈 #
1. 邮箱：linpelvis@gmail.com
2. issue：<a href="https://github.com/linpelvis/carrygo/issues">https://github.com/linpelvis/carrygo/issues</a>

