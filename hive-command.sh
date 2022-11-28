# 初始化hive元数据表
schematool -initSchema -dbType mysql

# hive创建外部表
create external table sogoulogs(id string,datatime string,userid string,searchname string,retorder string,cliorder string,cliurl string)
  STORED BY  'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
  WITH SERDEPROPERTIES("hbase.columns.mapping" = ":key,info:datatime,info:userid,info:searchname,info:retorder,info:cliorder,info:cliurl")
  TBLPROPERTIES("hbase.table.name" = "sogoulogs");

# hive集成hbase需要的包
cp hbase-client-1.2.0.jar /home/hadoop/app/hive/lib/
cp hbase-common-1.2.0.jar /home/hadoop/app/hive/lib/
cp hbase-server-1.2.0.jar /home/hadoop/app/hive/lib/
cp hbase-common-1.2.0-tests.jar /home/hadoop/app/hive/lib/
cp hbase-protocol-1.2.0.jar /home/hadoop/app/hive/lib/
cp htrace-core-3.1.0-incubating.jar /home/hadoop/app/hive/lib/
cp zookeeper-3.4.6.jar /home/hadoop/app/hive/lib/

# metastore 服务
bin/hive --service metastore 1>/dev/null 2>&1 &

# hive创建表, 根据文件数据
hive> show databases;
hive> create database djt;
hive> use djt;
hive> create table if not exists course(cid string,name string)ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED AS textfile;
hive> load data local inpath "/home/hadoop/shell/data/course.txt" into table course;
hive> select * from course;

# 条件删除数据
insert overwrite table course select * from course where cid is null