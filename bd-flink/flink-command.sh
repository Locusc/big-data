# 启动flink命令行 测试可以使用同spark一样的测试方式
bin/start-scala-shell.sh local

# standalone集群模式启动flink
bin/start-cluster.sh

# 命令行提交到任务到standalone集群
bin/flink run -c org.apache.flink.examples.java.wordcount.WordCount examples/batch/WordCount.jar --input hdfs://mycluster/test/djt.txt  --output hdfs://mycluster/test/output2

# 根据yid提交任务到yarn session(多Job共用yarn session方式, 需要手动关闭)
# yid 提交任务到指定yid的yarn session
bin/flink run -yid application_1669718008647_0001 -c org.apache.flink.examples.java.wordcount.WordCount examples/batch/WordCount.jar --input hdfs://mycluster/test/djt.txt  --output hdfs://mycluster/test/output4

# 创建一个yarn session
bin/yarn-session.sh -n 2 -s 2 -jm 1024 -tm 1024

# 查询yarn已存在的session
bin/yarn application -list

# 通过Flink run命令直接将Flink作业提交给YARN运行,
# 每次提交作业都会创建一个新的Flink集群,
# 任务之间相互独立、互不影响并且方便管理,任务执行完成之后创建的集群也会消失.

# -c --class 指定main class
# -C --classpath 指定classpath
# -m --jobManager 指定提交job给那个jobManager, 这里提交给YARN-yarn-cluster
# -yn --yarncontainer TaskManager的数量
# -ys --yarnslots 每个TaskManager的slot数量
# -yjm --yarnjobManagerMemory 运行jobManager的container的内存(单位mb)
# -ytm --yarntaskManagerMemory 运行每个TaskManager的container的内存(单位mb)
# -ynm --yarnname 应用名称
# -d --detached 后台执行
# -yqu --yarnqueue 指定YARN队列
# -p --parallelism 指定并行度
bin/flink run -m yarn-cluster -p 2 -yn 2 -ys 2 -yjm 1024 -ytm 1024  -c org.apache.flink.examples.java.wordcount.WordCount examples/batch/WordCount.jar --input hdfs://mycluster/test/djt.txt  --output hdfs://mycluster/test/output4
