package cn.locusc.bd.flink1170.confluence;

import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

/**
 * @author Jay
 * connect合流示例
 * 1、一次只能连接2条流
 * 2、流的数据类型可以不一样
 * 3、 连接后可以调用 map、flatmap、process来处理, 但是各处理各的
 * 2023/6/15
 */
public class ConnectConfluenceDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

        executionEnvironment.setParallelism(1);

        // DataStreamSource<Integer> source1 = executionEnvironment.fromElements(1, 2, 3);
        // DataStreamSource<String> source2 = executionEnvironment.fromElements("a", "b", "c");

        SingleOutputStreamOperator<Integer> source1 = executionEnvironment
                .socketTextStream("hadoop102", 7777)
                .map(Integer::parseInt);

        DataStreamSource<String> source2 = executionEnvironment.socketTextStream("hadoop102", 8888);

        ConnectedStreams<Integer, String> connect = source1.connect(source2);

        SingleOutputStreamOperator<String> result = connect.map(new CoMapFunction<Integer, String, String>() {
            @Override
            public String map1(Integer value) throws Exception {
                return "来源于数字流:" + value.toString();
            }

            @Override
            public String map2(String value) throws Exception {
                return "来源于字母流:" + value;
            }
        });

        result.print();

        executionEnvironment.execute();
    }

}
