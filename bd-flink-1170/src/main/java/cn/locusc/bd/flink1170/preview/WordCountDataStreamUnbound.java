package cn.locusc.bd.flink1170.preview;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author Jay
 * 无界流模拟 linux使用netcat监听端口
 * netcat -> nc -lk 7777 (可以在控制台输出)
 *
 * 并行度优先级:
 *     1.算子
 *     2.代码(ENV)
 *     3.提交任务时指定
 *     4.配置文件
 * 2023/6/12
 */
public class WordCountDataStreamUnbound {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

        // 本地环境运行时增加FlinkUI
        // 不指定并行度, 默认为CPU线程数
        // StreamExecutionEnvironment localEnvironmentWithWebUI = StreamExecutionEnvironment
        //         .createLocalEnvironmentWithWebUI(new Configuration());

        executionEnvironment.setParallelism(2);

        DataStreamSource<String> linesDS = executionEnvironment.socketTextStream("127.0.0.1", 7777);

        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = linesDS
                .flatMap((String value, Collector<Tuple2<String, Integer>> collector) -> {
                    String[] words = value.split(" ");
                    for (String word : words) {
                        collector.collect(new Tuple2<>(word, 1));
                    }
                })
                .setParallelism(2)
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(value -> value.f0)
                .sum(1);

        sum.print();

        executionEnvironment.execute();
    }

}
