package cn.locusc.bd.flink1170.watermark;

import cn.locusc.bd.flink1170.bean.WaterSensor;
import cn.locusc.bd.flink1170.operator.function.WaterSensorMapFunction;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @author Jay
 * 自定义水位线计算示例
 * 2023/6/20
 */
public class WatermarkCustomDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

        executionEnvironment.setParallelism(1);

        // 默认周期(水位线计算周期, 防止大数据量对水位线进行密集计算) 200ms
        executionEnvironment.getConfig().setAutoWatermarkInterval(2000);

        SingleOutputStreamOperator<WaterSensor> sensorDS = executionEnvironment
                .socketTextStream("hadoop102", 7777)
                .map(new WaterSensorMapFunction());

        WatermarkStrategy<WaterSensor> watermarkStrategy = WatermarkStrategy
                // 指定自定义的生成器
                // 1.自定义的 周期性生成
                // .<WaterSensor>forGenerator(ctx -> new MyPeriodWatermarkGenerator<>(3000L))
                // 2.自定义的 断点式生成
                .<WaterSensor>forGenerator(ctx -> new MyPunctatedWatermarkGenerator<>(3000L))
                .withTimestampAssigner((element, recordTimestamp) -> element.getTs() * 1000L);

        SingleOutputStreamOperator<WaterSensor> sensorDSwithWatermark = sensorDS.assignTimestampsAndWatermarks(watermarkStrategy);

        sensorDSwithWatermark.keyBy(WaterSensor::getId)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .process(
                        new ProcessWindowFunction<WaterSensor, String, String, TimeWindow>() {

                            @Override
                            public void process(String s, Context context, Iterable<WaterSensor> elements, Collector<String> out) throws Exception {
                                long startTs = context.window().getStart();
                                long endTs = context.window().getEnd();
                                String windowStart = DateFormatUtils.format(startTs, "yyyy-MM-dd HH:mm:ss.SSS");
                                String windowEnd = DateFormatUtils.format(endTs, "yyyy-MM-dd HH:mm:ss.SSS");

                                long count = elements.spliterator().estimateSize();

                                out.collect("key=" + s + "的窗口[" + windowStart + "," + windowEnd + ")包含" + count + "条数据===>" + elements.toString());
                            }
                        }
                )
                .print();

        executionEnvironment.execute();
    }

}
