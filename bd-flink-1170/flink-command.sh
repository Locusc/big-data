# xsync 可以封装同步脚本呢用来代替scp
xsync flink-1.17.0

# xcall 可以使用xcall查看集群的状态
xcall jps

# 命令上传任务
bin/flink run -m hadoop102:8081 -c cn.locusc.bd.flink1170.WordCountDataStreamUnbound workcount-task-1.0-SNAPSHOT.jar