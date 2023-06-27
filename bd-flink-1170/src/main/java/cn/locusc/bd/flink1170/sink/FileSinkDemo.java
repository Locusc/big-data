package cn.locusc.bd.flink1170.sink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.time.Duration;
import java.time.ZoneId;

/**
 * @author Jay
 * 文件sink示例
 * 2023/6/15
 */
public class FileSinkDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

        // 每个目录中, 都有并行度个数的文件在写入
        executionEnvironment.setParallelism(2);

        // 必须开启checkpoint, 否则一直都是 .inprogress
        executionEnvironment.enableCheckpointing(2000, CheckpointingMode.EXACTLY_ONCE);

        DataGeneratorSource<String> dataGeneratorSource = new DataGeneratorSource<>(
                new GeneratorFunction<Long, String>() {
                    @Override
                    public String map(Long value) throws Exception {
                        return "Number:" + value;
                    }
                },
                Long.MAX_VALUE,
                RateLimiterStrategy.perSecond(1000),
                Types.STRING
        );

        DataStreamSource<String> dataGen = executionEnvironment.fromSource(dataGeneratorSource, WatermarkStrategy.noWatermarks(), "data-generator");

        // 输出到文件系统
        FileSink<String> fieSink = FileSink
                // 输出行式存储的文件, 指定路径、指定编码
                .<String>forRowFormat(new Path("f:/tmp"), new SimpleStringEncoder<>("UTF-8"))
                // 输出文件的一些配置: 文件名的前缀、后缀
                .withOutputFileConfig(
                        OutputFileConfig.builder()
                                .withPartPrefix("atguigu-")
                                .withPartSuffix(".log")
                                .build()
                )
                // 按照目录分桶: 如下,就是每个小时一个目录
                .withBucketAssigner(new DateTimeBucketAssigner<>("yyyy-MM-dd HH", ZoneId.systemDefault()))
                // 文件滚动策略: 1分钟或1MB
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                .withRolloverInterval(Duration.ofMinutes(1))
                                .withMaxPartSize(new MemorySize(1024*1024))
                                .build()
                )
                .build();

        dataGen.sinkTo(fieSink);

        executionEnvironment.execute();
    }

}
