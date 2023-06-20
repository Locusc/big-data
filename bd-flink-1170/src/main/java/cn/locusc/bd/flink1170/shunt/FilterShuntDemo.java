package cn.locusc.bd.flink1170.shunt;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author Jay
 * 利用filter进行分流(同一个数据, 要被处理两遍, 调用两次filter)
 * 使用DataStream API进行处理
 * 2023/6/15
 */
public class FilterShuntDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        executionEnvironment.setParallelism(1);

        DataStreamSource<String> socketDS = executionEnvironment.socketTextStream("hadoop102", 7777);

        SingleOutputStreamOperator<String> even = socketDS.filter(value -> Integer.parseInt(value) % 2 == 0);
        SingleOutputStreamOperator<String> odd = socketDS.filter(value -> Integer.parseInt(value) % 2 == 1);

        even.print("偶数流");
        odd.print("奇数流");

        executionEnvironment.execute();
    }

}
