package cn.locusc.bd.flink.news.datastream;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * @author Jay
 * flume -> kafka -> flink -> mysql
 * 2022/11/29
 */
public class KafkaDataSource {

    public static void main(String[] args) throws Exception {
        // 获取Flink的运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // kafka配置参数
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "hadoop01:9092,hadoop02:9092,hadoop03:9092");
        properties.setProperty("group.id", "sogoulogs");

        // kafka消费者
        FlinkKafkaConsumer<String> myConsumer = new FlinkKafkaConsumer<>(
                "sogoulogs",
                new SimpleStringSchema(),
                properties
        );

        DataStream<String> stream = env.addSource(myConsumer);

        // 对数据进行过滤
        DataStream<String> filter = stream.filter((value) -> value.split(",").length==6);

        DataStream<Tuple2<String, Integer>> newsCounts = filter
                .flatMap((FlatMapFunction<String, Tuple2<String, Integer>>) (in, out) -> {
                    String[] tokens = in.toLowerCase().split(",");
                    out.collect(new Tuple2<>(tokens[2], 1));
                })
                .returns(TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {}))
                .keyBy(0)
                .sum(1);

        // 自定义MySQL sink
        newsCounts.addSink(new MysqlDataSink(
                f -> {
                    String  name = f.f0.replaceAll("[\\[\\]]", "");
                    int count = f.f1;

                    String querySQL="select 1 from newscount "+" where name = '"+name+"'";

                    String updateSQL="update newscount set count = "+count+" where name = '"+name+"'";

                    String insertSQL = "insert into newscount(name,count) values('"+name+"',"+count+")";

                    return new Tuple3<>(querySQL, updateSQL, insertSQL);
                }
        ));

        DataStream<Tuple2<String, Integer>> periodCounts = filter
                .flatMap((FlatMapFunction<String, Tuple2<String, Integer>>) (in, out) -> {
                    String[] tokens = in.toLowerCase().split(",");
                    out.collect(new Tuple2<>(tokens[0], 1));
                })
                .returns(TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {}))
                .keyBy(0)
                .sum(1);

        // 自定义MySQL sink
        periodCounts.addSink(new MysqlDataSink(
                f -> {
                    String  logtime = f.f0;
                    int count = f.f1;

                    String querySQL="select 1 from periodcount "+" where logtime = '"+logtime+"'";

                    String updateSQL="update periodcount set count = "+count+" where logtime ='"+logtime+"'";

                    String insertSQL = "insert into periodcount(logtime,count) values('"+logtime+"',"+count+")";
                    return new Tuple3<>(querySQL, updateSQL, insertSQL);
                }
        ));

        // 执行flink 程序
        env.execute("FlinkDataStreamDemo");
    }

}
