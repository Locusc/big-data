package cn.locusc.bd.flink1170.operator.aggregate;

import cn.locusc.bd.flink1170.bean.WaterSensor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author Jay
 * 简单的聚合实例
 *  1、keyby之后才能调用
 *  2、分组内的聚合: 对同一个key的数据进行聚合
 * 2023/6/14
 */
public class SimpleAggregateDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

        executionEnvironment.setParallelism(1);

        DataStreamSource<WaterSensor> sensorDS = executionEnvironment.fromElements(
                new WaterSensor("s1", 1L, 1),
                new WaterSensor("s1", 11L, 11),
                new WaterSensor("s2", 2L, 2),
                new WaterSensor("s3", 3L, 3)
        );

        KeyedStream<WaterSensor, String> sensorKS = sensorDS
                .keyBy(new KeySelector<WaterSensor, String>() {
                    @Override
                    public String getKey(WaterSensor value) throws Exception {
                        return value.getId();
                    }
                });

        // 传位置索引的, 适用于Tuple类型, POJO不行
        // SingleOutputStreamOperator<WaterSensor> result = sensorKS.sum(2);
        // SingleOutputStreamOperator<WaterSensor> result = sensorKS.sum("vc");

        // max|maxBy的区别: 同min
        // max: 只会取比较字段的最大值, 非比较字段保留第一次的值
        // maxby: 取比较字段的最大值, 同时非比较字段取最大值这条数据的值

        // SingleOutputStreamOperator<WaterSensor> result = sensorKS.max("vc");
        // SingleOutputStreamOperator<WaterSensor> result = sensorKS.min("vc");
        // SingleOutputStreamOperator<WaterSensor> result = sensorKS.maxBy("vc");
        SingleOutputStreamOperator<WaterSensor> singleOutputStreamOperator = sensorKS.minBy("vc");

        singleOutputStreamOperator.print();

        executionEnvironment.execute();
    }

}
