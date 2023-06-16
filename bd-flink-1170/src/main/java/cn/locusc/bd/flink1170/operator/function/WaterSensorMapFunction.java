package cn.locusc.bd.flink1170.operator.function;

import cn.locusc.bd.flink1170.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;

/**
 * @author Jay
 * 提取公共的map function
 * 2023/6/15
 */
public class WaterSensorMapFunction implements MapFunction<String,WaterSensor> {

    @Override
    public WaterSensor map(String value) throws Exception {
        String[] split = value.split(",");
        return new WaterSensor(split[0], Long.valueOf(split[1]), Integer.valueOf(split[2]));
    }

}
