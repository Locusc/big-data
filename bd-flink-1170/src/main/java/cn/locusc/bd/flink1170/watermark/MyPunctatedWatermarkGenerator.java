package cn.locusc.bd.flink1170.watermark;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;

/**
 * @author Jay
 * 断点式的触发水位线计算主要体现在
 * org.apache.flink.api.common.eventtime.WatermarkGenerator#onEvent(java.lang.Object, long, org.apache.flink.api.common.eventtime.WatermarkOutput)
 * 2023/6/20
 */
public class MyPunctatedWatermarkGenerator<T> implements WatermarkGenerator<T> {

    // 乱序等待时间
    private final long delayTs;

    // 用来保存当前为止最大的事件时间
    private long maxTs;

    public MyPunctatedWatermarkGenerator(long delayTs) {
        this.delayTs = delayTs;
        this.maxTs = Long.MIN_VALUE + this.delayTs + 1;
    }

    /**
     * 每条数据来, 都会调用一次: 用来提取最大的事件时间, 保存下来, 并发射watermark
     * @param eventTimestamp 提取到的数据的 事件时间
     */
    @Override
    public void onEvent(T event, long eventTimestamp, WatermarkOutput output) {
        maxTs = Math.max(maxTs, eventTimestamp);
        output.emitWatermark(new Watermark(maxTs - delayTs - 1));
        System.out.println("调用onEvent方法，获取目前为止的最大时间戳=" + maxTs+",watermark="+(maxTs - delayTs - 1));
    }

    /**
     * 周期性调用: 不需要
     */
    @Override
    public void onPeriodicEmit(WatermarkOutput output) {

    }

}
