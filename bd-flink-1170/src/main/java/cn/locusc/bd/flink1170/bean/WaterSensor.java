package cn.locusc.bd.flink1170.bean;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * @author Jay
 * 模拟水位传感器
 * 2023/6/14
 */
@Data
@AllArgsConstructor
public class WaterSensor {

    /**
     * 水位传感器类型
     */
    public String id;

    /**
     * 传感器记录时间戳
     */
    public Long ts;

    /**
     * 水位记录
     */
    public Integer vc;
    
}
