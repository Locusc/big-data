# zookeeper单节点启动以及状态查看
/home/hadoop/app/zookeeper/bin/zkServer.sh start
/home/hadoop/app/zookeeper/bin/zkServer.sh status

# yarn备用节点资源管理器启动关闭命令
sbin/yarn-daemon.sh start resourcemanager
sbin/yarn-daemon.sh stop resourcemanager

# hadoop集群, hbase集群, 以及yarn集群启动命令
sbin/start-dfs.sh
bin/start-hbase.sh
sbin/start-yarn.sh

# 进入hbase命令行
hbase shell

# hive命令行
bin/hive
# hive服务端

# mapreduce自带workcount测试命令
hadoop jar share/hadoop/mapreduce/hadoop-mapreduce-examples-2.9.1.jar workcount /test/djt.txt /test/out/

# kafka server单节点启动命令
bin/kafka-server-start.sh config/server.properties 1>/dev/null 2>&1 &
# kafka消费者 指定主题
bin/kafka-console-consumer.sh --bootstrap-server 127.0.0.1:9092 --topic sogoulogs
# 创建topic
bin/kafka-topics.sh --zookeeper 127.0.0.1:2181 --create --topic djt --replication-factor 3 --partitions 3
# 查看kafka现有主题
bin/kafka-topics.sh --zookeeper 127.0.0.1:2181 --list
# 查看kafka主题详情包含(分区, 副本, leader)
bin/kafka-topics.sh --zookeeper 127.0.0.1:2181 --describe --topic djt
# 删除kafka主题
bin/kafka-topics.sh --zookeeper 127.0.0.1:2181 --delete --topic djt

# flume agent测试命令
# flume-ng脚本后面的agent代表启动Flume进程,
# -n指定的是配置文件中Agent的名称,
# -c指定配置文件所在目录,
# -f指定具体的配置文件,
# -Dflume.root.logger=INFO, console指的是控制台打印INFO, console级别的日志信息
bin/flume-ng agent -n agent -c conf -f conf/flume-conf.properties -Dflume.root.logger=INFO,console
# taildir-file-selector-avro.properties
bin/flume-ng agent -n agent1 -c conf -f conf/taildir-file-selector-avro.properties -Dflume.root.logger=INFO,console 1>/dev/null 2>&1 &
# avro-file-selector-logger.properties
bin/flume-ng agent -n agent1 -c conf -f conf/avro-file-selector-logger.properties -Dflume.root.logger=INFO,console 1>/dev/null 2>&1 &
# avro-file-selector-kafka.properties
bin/flume-ng agent -n agent1 -c conf -f conf/avro-file-selector-kafka.properties -Dflume.root.logger=INFO,console
# avro-file-selector-hbase.properties
bin/flume-ng agent -n agent1 -c conf -f conf/avro-file-selector-hbase.properties -Dflume.root.logger=INFO,console
# avro-file-selector-hbase-kafka.properties
bin/flume-ng agent -n agent1 -c conf -f conf/avro-file-selector-hbase-kafka.properties -Dflume.root.logger=INFO,console