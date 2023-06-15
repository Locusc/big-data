package cn.locusc.bd.flink1170.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author Jay
 * 数据生成demo
 *     setParallelism:
 *         如果有n个并行度, 最大值设为a,
 *         将数值均分成n份, a/n, 比如, 最大100, 并行度2, 每个并行度生成50个,
 *         其中一个是0-49, 另一个50-99
 *
 *     数据生成器Source, 四个参数:
 *         第一个: GeneratorFunction接口, 需要实现,重写map方法,输入类型固定是Long
 *         第二个: long类型, 自动生成的数字序列(从0自增)的最大值(小于), 达到这个值就停止了
 *         第三个: 限速策略, 比如每秒生成几条数据
 *         第四个: 返回的类型
 * 2023/6/14
 */
public class DataGeneratorSourceDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

        executionEnvironment.setParallelism(2);

        DataGeneratorSource<String> dataGeneratorSource = new DataGeneratorSource<String>(
                new GeneratorFunction<Long, String>() {
                    @Override
                    public String map(Long value) throws Exception {
                        return "generator-" + value;
                    }
                },
                Integer.MAX_VALUE,
                RateLimiterStrategy.perSecond(10),
                Types.STRING
        );

        executionEnvironment.fromSource(dataGeneratorSource, WatermarkStrategy.noWatermarks(), "generator-source")
                .print();

        executionEnvironment.execute();
    }

}
