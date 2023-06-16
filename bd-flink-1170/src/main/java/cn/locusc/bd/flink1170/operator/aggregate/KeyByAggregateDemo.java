package cn.locusc.bd.flink1170.operator.aggregate;

import cn.locusc.bd.flink1170.bean.WaterSensor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author Jay
 * 分区实例
 * 按照id分组
 * 要点:
 *  1、返回的是一个KeyedStream, 键控流
 *  2、keyby不是转换算子, 只是对数据进行重分区, 不能设置并行度
 *  3、分组与分区的关系:
 *    1） keyby是对数据分组, 保证相同key的数据 在同一个分区(子任务)
 *    2） 分区: 一个子任务可以理解为一个分区, 一个分区(子任务)中可以存在多个分组(key)
 * 2023/6/14
 */
public class KeyByAggregateDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

        executionEnvironment.setParallelism(2);

        DataStreamSource<WaterSensor> sensorDS = executionEnvironment.fromElements(
                new WaterSensor("s1", 1L, 1),
                new WaterSensor("s1", 11L, 11),
                new WaterSensor("s2", 2L, 2),
                new WaterSensor("s3", 3L, 3)
        );

        sensorDS.keyBy(new KeySelector<WaterSensor, String>() {
            @Override
            public String getKey(WaterSensor value) throws Exception {
                return value.getId();
            }
        });

        sensorDS.print();

        executionEnvironment.execute();
    }

}
