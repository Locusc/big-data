package cn.locusc.bd.flink1170.operator.partition;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author Jay
 * 自定义分区示例
 * 2023/6/15
 */
public class PartitionCustomDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        executionEnvironment.setParallelism(2);

        DataStreamSource<String> socketDS = executionEnvironment.socketTextStream("hadoop102", 7777);

        socketDS.partitionCustom(new HintPartitioner(), k -> k)
                .print();

        executionEnvironment.execute();
    }

    public static class HintPartitioner implements Partitioner<String> {

        @Override
        public int partition(String key, int numPartitions) {
            return Integer.parseInt(key) % numPartitions;
        }

    }

}
