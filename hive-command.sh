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