package cn.locusc.bd.flink1170.watermark;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;

/**
 * @author Jay
 * 周期性的触发水位线计算主要体现在org.apache.flink.api.common.ExecutionConfig#setAutoWatermarkInterval(long)
 * org.apache.flink.api.common.eventtime.WatermarkGenerator#onPeriodicEmit(org.apache.flink.api.common.eventtime.WatermarkOutput)
 * 2023/6/20
 */
public class MyPeriodWatermarkGenerator<T> implements WatermarkGenerator<T> {

    // 乱序等待时间
    private final long delayTs;

    // 用来保存 当前为止 最大的事件时间
    private long maxTs;

    public MyPeriodWatermarkGenerator(long delayTs) {
        this.delayTs = delayTs;
        this.maxTs = Long.MIN_VALUE + this.delayTs + 1;
    }

    /**
     * 每条数据来，都会调用一次: 用来提取最大的事件时间，保存下来
     * @param eventTimestamp 提取到的数据的 事件时间
     */
    @Override
    public void onEvent(T event, long eventTimestamp, WatermarkOutput output) {
        maxTs = Math.max(maxTs, eventTimestamp);
        System.out.println("调用onEvent方法，获取目前为止的最大时间戳=" + maxTs);
    }

    /**
     * 周期性调用: 发射 watermark
     */
    @Override
    public void onPeriodicEmit(WatermarkOutput output) {
        output.emitWatermark(new Watermark(maxTs - delayTs - 1));
        System.out.println("调用onPeriodicEmit方法，生成watermark=" + (maxTs - delayTs - 1));
    }

}
