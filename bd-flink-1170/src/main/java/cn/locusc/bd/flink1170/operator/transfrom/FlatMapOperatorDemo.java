package cn.locusc.bd.flink1170.operator.transfrom;

import cn.locusc.bd.flink1170.bean.WaterSensor;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author Jay
 * 扁平映射算子实例
 *    flatMap: 一进多出(包含0出)
 *      对于s1的数据, 一进一出
 *      对于s2的数据, 一进2出
 *      对于s3的数据, 一进0出（类似于过滤的效果）
 *
 *    map怎么控制一进一出：
 *      => 使用return
 *
 *    flatMap怎么控制的一进多出
 *      => 通过Collector来输出, 调用几次就输出几条
 * 2023/6/14
 */
public class FlatMapOperatorDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

        executionEnvironment.setParallelism(1);

        DataStreamSource<WaterSensor> sensorDS = executionEnvironment.fromElements(
                new WaterSensor("s1", 1L, 1),
                new WaterSensor("s1", 11L, 11),
                new WaterSensor("s2", 2L, 2),
                new WaterSensor("s3", 3L, 3)
        );

        SingleOutputStreamOperator<String> flatmap = sensorDS.flatMap(new FlatMapFunction<WaterSensor, String>() {

            @Override
            public void flatMap(WaterSensor value, Collector<String> out) throws Exception {
                if ("s1".equals(value.getId())) {
                    // 如果是s1, 输出 vc
                    out.collect(value.getVc().toString());
                } else if ("s2".equals(value.getId())) {
                    // 如果是s2, 分别输出ts和vc
                    out.collect(value.getTs().toString());
                    out.collect(value.getVc().toString());
                }
            }

        });

        flatmap.print();


        executionEnvironment.execute();
    }
    
}
