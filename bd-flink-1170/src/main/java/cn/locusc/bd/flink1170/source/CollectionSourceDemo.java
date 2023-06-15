package cn.locusc.bd.flink1170.source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author Jay
 * 集合数据源示例
 * 2023/6/14
 */
public class CollectionSourceDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

        executionEnvironment.setParallelism(1);

        // executionEnvironment.fromCollection(Arrays.asList("1", "2", "3"));

        // 从集合读取数据
        DataStreamSource<String> dataStreamSource = executionEnvironment.fromElements("1", "2", "3");

        dataStreamSource.print();

        executionEnvironment.execute();
    }

}
