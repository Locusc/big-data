package cn.locusc.bd.flink1170.env;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author Jay
 * 执行环境创建示例
 *     1、默认env.execute()触发一个flink job:
 *         => 一个main方法可以调用多个execute, 但是没意义, 指定到第一个就会阻塞住
 *     2、env.executeAsync()异步触发, 不阻塞
 *         => 一个main方法里 executeAsync()个数 = 生成的flink job数
 *     3、思考:
 *         yarn-application集群, 提交一次, 集群里会有几个flink job?
 *         => 取决于调用了n个executeAsync()
 *         => 对应application集群里, 会有n个job
 *         => 对应Jobmanager当中, 会有n个JobMaster
 * 2023/6/14
 */
public class EnvironmentDemo {

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration()
                .set(RestOptions.BIND_PORT, "8081");

        // 自动识别远程环境还是本地环境
        // StreamExecutionEnvironment.getExecutionEnvironment();

        // 创建本地环境
        // StreamExecutionEnvironment.createLocalEnvironment();

        // 创建本地环境带flinkUI
        // StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        // 创建远程环境
        // StreamExecutionEnvironment.createRemoteEnvironment("localhost", 8081, "flink-streaming-java-0.1.jar");

        // 自动创建执行环境, 并且是同外部配置参数
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment(configuration);

        // 设置运行模式, 默认为流式模式(STREAMING)
        // 一般情况为外部执行(-Dexecution.runtime-mode=BATCH)
        executionEnvironment.setRuntimeMode(RuntimeExecutionMode.BATCH);

        executionEnvironment.readTextFile("input/word.txt")
                .flatMap(
                        (String value, Collector<Tuple2<String, Integer>> out) -> {
                            String[] words = value.split(" ");
                            for (String word : words) {
                                out.collect(Tuple2.of(word, 1));
                            }
                        }
                )
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(value -> value.f0)
                .sum(1)
                .print();

        executionEnvironment.execute();

        // executionEnvironment.executeAsync();
    }

}
