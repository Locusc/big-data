package cn.locusc.bd.flink1170.sink;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;

/**
 * @author Jay
 * key以及value同时序列化示例
 * 如果要指定写入kafka的key
 * 可以自定义序列器:
 * 1、实现一个接口, 重写序列化方法
 * 2、指定key, 转成 字节数组
 * 3、指定value, 转成 字节数组
 * 4、返回一个ProducerRecord对象, 把key、value放进去
 * 2023/6/15
 */
public class KafkaWithKeySinkDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

        executionEnvironment.setParallelism(1);
        executionEnvironment.enableCheckpointing(2000, CheckpointingMode.EXACTLY_ONCE);
        executionEnvironment.setRestartStrategy(RestartStrategies.noRestart());

        SingleOutputStreamOperator<String> sensorDS = executionEnvironment.socketTextStream("hadoop102", 7777);

        KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
                .setBootstrapServers("hadoop102:9092,hadoop103:9092,hadoop104:9092")
                .setRecordSerializer(

                        new KafkaRecordSerializationSchema<String>() {
                            @Nullable
                            @Override
                            public ProducerRecord<byte[], byte[]> serialize(String element, KafkaSinkContext context, Long timestamp) {
                                String[] split = element.split(",");
                                byte[] key = split[0].getBytes(StandardCharsets.UTF_8);
                                byte[] value = element.getBytes(StandardCharsets.UTF_8);
                                return new ProducerRecord<>("ws", key, value);
                            }
                        }

                )
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .setTransactionalIdPrefix("locusc-")
                .setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, 10 * 60 * 1000 + "")
                .build();

        sensorDS.sinkTo(kafkaSink);

        executionEnvironment.execute();
    }

}
