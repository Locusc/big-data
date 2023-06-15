package cn.locusc.bd.flink1170.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author Jay
 * kafka数据源实例
 *
 *   kafka消费者的参数：
 *      auto.reset.offsets
 *          earliest: 如果有offset, 从offset继续消费; 如果没有offset, 从最早消费
 *          latest  : 如果有offset, 从offset继续消费; 如果没有offset, 从最新消费
 *
 *   flink的kafkaSource, offset消费策略: OffsetsInitializer, 默认是 earliest
 *          earliest: 一定从最早消费
 *          latest  : 一定从最新消费
 *
 * 2023/6/14
 */
public class KafkaSourceDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                // 指定kafka节点的地址和端口
                .setBootstrapServers("127.0.0.1:9092,127.0.0.1:9093,127.0.0.1:9094")
                // 指定消费者组的id
                .setGroupId("locusc")
                // 指定消费的topic
                .setTopics("topic_1")
                // 指定反序列化器, 这个是反序列化value
                .setStartingOffsets(OffsetsInitializer.latest())
                // flink消费kafka的策略
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        executionEnvironment.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(),"kafka-source")
                .print();

        executionEnvironment.execute();
    }

}
