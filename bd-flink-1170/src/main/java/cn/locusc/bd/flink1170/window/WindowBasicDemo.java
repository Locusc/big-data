package cn.locusc.bd.flink1170.window;

import cn.locusc.bd.flink1170.bean.WaterSensor;
import cn.locusc.bd.flink1170.operator.function.WaterSensorMapFunction;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.*;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * @author Jay
 * flink窗口基础示例
 * 2023/6/19
 */
public class WindowBasicDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

        executionEnvironment.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> sensorDS = executionEnvironment.socketTextStream("127.0.0.1", 7777)
                .map(new WaterSensorMapFunction());

        KeyedStream<WaterSensor, String> sensorKS = sensorDS.keyBy(WaterSensor::getId);

        // 1. 指定窗口分配器: 指定用哪一种窗口 ---  时间 or 计数? 滚动、滑动、会话?
        // 1.1 没有keyBy的窗口: 窗口内的 所有数据 进入同一个 子任务, 并行度只能为1
        // sensorKS.windowAll();

        // 1.2 有keyBy的窗口: 每个key上都定义了一组窗口，各自独立地进行统计计算
        //  基于时间的
        sensorKS.window(TumblingEventTimeWindows.of(Time.seconds(10))); // 滚动窗口,窗口长度10s
        sensorKS.window(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(2))); // 滑动窗口, 窗口长度10s, 滑动步长2s
        sensorKS.window(ProcessingTimeSessionWindows.withGap(Time.seconds(5))); // 会话窗口, 超时间隔5s


        // 基于计数的
        sensorKS.countWindow(5);  // 滚动窗口, 窗口长度=5个元素
        sensorKS.countWindow(5,2); // 滑动窗口, 窗口长度=5个元素, 滑动步长=2个元素
        sensorKS.window(GlobalWindows.create());  // 全局窗口, 计数窗口的底层就是用的这个, 需要自定义的时候才会用

        // 2.指定 窗口函数: 窗口内数据的 计算逻辑
        WindowedStream<WaterSensor, String, TimeWindow> sensorWS = sensorKS.window(TumblingProcessingTimeWindows.of(Time.seconds(10)));
        // sensorWS.reduce();
        // sensorWS.aggregate();

        // 全窗口函数: 数据来了不计算,存起来,窗口触发的时候,计算并输出结果
        // sensorWS.process();
        executionEnvironment.execute();
    }

}
