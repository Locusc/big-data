package cn.locusc.bd.flink1170.operator.transfrom;

import cn.locusc.bd.flink1170.bean.WaterSensor;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author Jay
 * 过滤算子实例(true保留, false过滤)
 * 2023/6/14
 */
public class FilterOperatorDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

        executionEnvironment.setParallelism(1);

        DataStreamSource<WaterSensor> sensorDS = executionEnvironment.fromElements(
                new WaterSensor("s1", 1L, 1),
                new WaterSensor("s1", 11L, 11),
                new WaterSensor("s2", 2L, 2),
                new WaterSensor("s3", 3L, 3)
        );

        SingleOutputStreamOperator<WaterSensor> filter = sensorDS.filter(new FilterFunctionImpl("s1"));

        filter.print();

        executionEnvironment.execute();
    }

    public static class FilterFunctionImpl implements FilterFunction<WaterSensor> {

        public String id;

        public FilterFunctionImpl(String id) {
            this.id = id;
        }

        @Override
        public boolean filter(WaterSensor value) throws Exception {
            return this.id.equals(value.getId());
        }

    }

}
