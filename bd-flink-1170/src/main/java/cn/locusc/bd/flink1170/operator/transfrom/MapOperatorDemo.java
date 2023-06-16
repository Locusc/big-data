package cn.locusc.bd.flink1170.operator.transfrom;

import cn.locusc.bd.flink1170.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author Jay
 * map算子实例
 * 2023/6/14
 */
public class MapOperatorDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

        executionEnvironment.setParallelism(1);

        DataStreamSource<WaterSensor> sensorDS = executionEnvironment.fromElements(
                new WaterSensor("s1", 1L, 1),
                new WaterSensor("s2", 2L, 2),
                new WaterSensor("s3", 3L, 3)
        );

        // 匿名实现类
        // SingleOutputStreamOperator<String> map = sensorDS.map(new MapFunction<WaterSensor, String>() {
        //     @Override
        //     public String map(WaterSensor value) throws Exception {
        //         return value.getId();
        //     }
        // });

        // lambda表达式
        // SingleOutputStreamOperator<String> map = sensorDS.map(WaterSensor::getId);

        // 定义一个类来实现MapFunction
        SingleOutputStreamOperator<String> singleOutputStreamOperator = sensorDS.map(new MapFunctionImpl());

        singleOutputStreamOperator.print();

        executionEnvironment.execute();
    }

    public static class MapFunctionImpl implements MapFunction<WaterSensor, String> {

        @Override
        public String map(WaterSensor value) throws Exception {
            return value.getId();
        }

    }

}
