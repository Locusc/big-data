# A master URL must be set in your configuration
# 需要指定spark的mater URL
# local 本地单线程
# local[K] 本地多线程(指定K个内核)
# local[*] 本地多线程指定所有可用内核()
# spark://HOST:PORT 连接到指定的Spark standalone cluster master,需要指定端口.
# mesos://HOST:PORT 连接到指定的Mesos集群,需要指定端口.
# yarn-client客户端模式 连接到YARN集群. 需要配置HADOOP_CONF_DIR.
# yarn-cluster集群模式 连接到YARN集群. 需要配置HADOOP_CONF_DIR.

# Exception in thread "main" java.lang.NoSuchMethodError: io.netty.buffer.PooledByteBufAllocator.metric()Lio/netty/buffer/PooledByteBufAllocatorMetric;
# spark的netty包可能和其他包的netty版本冲突

# Could not locate executable null\bin\winutils.exe in the Hadoop binaries
# 缺少hadoop-common-2.2.0-bin/winutils.exe

# createFileWithMode0(Ljava/lang/String;JJJI)Ljava/io/FileDescriptor;
# hadoop-common-2.2.0-bin/hadoop.dll存在, hadoop认为是集群环境

# 进入spark shell
bin/spark-shell

# 启动spark集群
sbin/start-all.sh

# 启动spark master节点
sbin/start-master.sh

# 通过spark-submit脚本提交任务-standalone
bin/spark-submit --master spark://hadoop01:7077,hadoop02:7077 --class cn.locusc.spark.action.CustomWordCount /home/hadoop/shell/lib/bd-spark-1.0-SNAPSHOT.jar /test/djt.txt /test/output1
# 通过spark-submit脚本提交任务-yarn
bin/spark-submit --master yarn --class cn.locusc.spark.action.CustomWordCount /home/hadoop/shell/lib/bd-spark-1.0-SNAPSHOT.jar /test/djt.txt /test/output2

# 统计测试scala函数
val line = sc.textFile("/home/hadoop/app/spark/djt.txt")
line.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).collect().foreach(println)

# spark-sql查询关系型数据库
val df = spark
  .read
  .format("jdbc")
  .option("url", "jdbc:mysql://39.107.96.199:3306/mercy")
  .option("dbtable", "newscount")
  .option("user", "root")
  .option("password", "wdnmd123")
  .load()

# spark集成hbase
cp hbase-client-1.2.0.jar /home/hadoop/app/spark/jars/
cp hbase-common-1.2.0.jar /home/hadoop/app/spark/jars/
cp hbase-protocol-1.2.0.jar /home/hadoop/app/spark/jars/
cp hbase-server-1.2.0.jar /home/hadoop/app/spark/jars/
cp htrace-core-3.1.0-incubating.jar /home/hadoop/app/spark/jars/
cp metrics-core-2.2.0.jar /home/hadoop/app/spark/jars/
cp hive-hbase-handler-2.3.7.jar /home/hadoop/app/spark/jars/
cp mysql-connector-java-5.1.38.jar /home/hadoop/app/spark/jars/


