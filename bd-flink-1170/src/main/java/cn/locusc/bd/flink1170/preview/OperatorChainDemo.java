package cn.locusc.bd.flink1170.preview;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author Jay
 * 算子链实例 (1.计算任务繁重, 2.定位算子问题) 类似于spark的stage
 *
 *  1.算子之间的传输关系:
 *      一对一
 *      重分区
 *
 *  2.算子 串在一起的条件:
 *     1） 一对一
 *     2） 并行度相同
 *
 *  3.关于算子链的api:
 *     1）全局禁用算子链: env.disableOperatorChaining();
 *     2）某个算子不参与链化: 算子A.disableChaining(), 算子A不会与 前面 和 后面的 算子 串在一起
 *     3）从某个算子开启新链条: 算子A.startNewChain(), 算子A不与 前面串在一起 从A开始正常链化
 *
 * 2023/6/13
 */
public class OperatorChainDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment
                .createLocalEnvironmentWithWebUI(new Configuration());

        executionEnvironment.setParallelism(1);

        // 全局禁用算子链
        executionEnvironment.disableOperatorChaining();

        DataStreamSource<String> linesDS = executionEnvironment.socketTextStream("127.0.0.1", 7777);

        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = linesDS
                .disableChaining()
                .flatMap((String value, Collector<String> collector) -> {
                    String[] words = value.split(" ");
                    for (String word : words) {
                        collector.collect(word);
                    }
                })
                .startNewChain()
                // .disableChaining()
                // .setParallelism(2)
                .returns(Types.STRING)
                .map(m -> Tuple2.of(m, 1))
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(value -> value.f0)
                .sum(1);

        sum.print();

        executionEnvironment.execute();
    }

}
