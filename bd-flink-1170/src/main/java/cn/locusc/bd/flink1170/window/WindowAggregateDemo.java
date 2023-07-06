package cn.locusc.bd.flink1170.window;

import cn.locusc.bd.flink1170.bean.WaterSensor;
import cn.locusc.bd.flink1170.operator.function.WaterSensorMapFunction;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * @author Jay
 * 窗口函数:增量聚合Aggregate
 * 1、属于本窗口的第一条数据来, 创建窗口, 创建累加器
 * 2、增量聚合: 来一条计算一条, 调用一次add方法
 * 3、窗口输出时调用一次getresult方法
 * 4、输入、中间累加器、输出 类型可以不一样, 非常灵活
 * 2023/6/19
 */
public class WindowAggregateDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

        executionEnvironment.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> sensorDS = executionEnvironment
                .socketTextStream("127.0.0.1", 7777)
                .map(new WaterSensorMapFunction());


        KeyedStream<WaterSensor, String> sensorKS = sensorDS.keyBy(WaterSensor::getId);

        // 1. 窗口分配器
        WindowedStream<WaterSensor, String, TimeWindow> sensorWS = sensorKS.window(
                TumblingProcessingTimeWindows.of(Time.seconds(10)));

        // 2. 窗口函数: 增量聚合Aggregate
        SingleOutputStreamOperator<String> aggregate = sensorWS.aggregate(
                /**
                 * 第一个类型: 输入数据的类型
                 * 第二个类型: 累加器的类型，存储的中间计算结果的类型
                 * 第三个类型: 输出的类型
                 */
                new AggregateFunction<WaterSensor, Integer, String>() {

                    /**
                     * 创建累加器，初始化累加器
                     */
                    @Override
                    public Integer createAccumulator() {
                        System.out.println("创建累加器");
                        return 0;
                    }

                    /**
                     * 聚合逻辑
                     */
                    @Override
                    public Integer add(WaterSensor value, Integer accumulator) {
                        System.out.println("调用add方法,value="+value);
                        return accumulator + value.getVc();
                    }

                    /**
                     * 获取最终结果, 窗口触发时输出
                     */
                    @Override
                    public String getResult(Integer accumulator) {
                        System.out.println("调用getResult方法");
                        return accumulator.toString();
                    }

                    /**
                     * 只有会话窗口才会用到
                     */
                    @Override
                    public Integer merge(Integer a, Integer b) {
                        System.out.println("调用merge方法");
                        return null;
                    }
                }
        );

        aggregate.print();

        executionEnvironment.execute();
    }

}
