package cn.locusc.bd.flink1170.confluence;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author Jay
 * union合流示例
 * 1、 流的数据类型必须一致
 * 2、 一次可以合并多条流
 * 2023/6/15
 */
public class UnionConfluenceDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

        executionEnvironment.setParallelism(1);

        DataStreamSource<Integer> source1 = executionEnvironment.fromElements(1, 2, 3);
        DataStreamSource<Integer> source2 = executionEnvironment.fromElements(11, 22, 33);
        DataStreamSource<String> source3 = executionEnvironment.fromElements("111", "222", "333");

        //  DataStream<Integer> union = source1.union(source2).union(source3.map(r -> Integer.valueOf(r)));
        DataStream<Integer> union = source1.union(source2, source3.map(Integer::valueOf));

        union.print();

        executionEnvironment.execute();
    }

}
