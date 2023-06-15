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
 * 任务槽示例
 *
 *  1）slot特点:
 *  1）均分隔离内存, 不隔离cpu
 *  2）可以共享:
 *  同一个job中, 不同算子的子任务 才可以共享 同一个slot, 同时在运行的
 *  前提是, 属于同一个 slot共享组, 默认都是"default"
 *
 *  2) slot数量 与 并行度 的关系
 *  1）slot是一种静态的概念, 表示最大的并发上限
 *  并行度是一种动态的概念, 表示 实际运行 占用了 几个
 *
 *  2）要求: slot数量 >= job并行度（算子最大并行度）, job才能运行
 *  TODO 注意: 如果是yarn模式, 动态申请
 *  --》 TODO 申请的TM数量 = job并行度 / 每个TM的slot数, 向上取整
 *  比如session: 一开始 0个TaskManager, 0个slot
 *  --》 提交一个job, 并行度10
 *  --》 10/3, 向上取整, 申请4个tm
 *  --》 使用10个slot, 剩余2个slot
 *
 * 2023/6/13
 */
public class SlotSharingGroupDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment
                .createLocalEnvironmentWithWebUI(new Configuration());

        executionEnvironment.setParallelism(1);

        // 全局禁用算子链
        executionEnvironment.disableOperatorChaining();

        DataStreamSource<String> linesDS = executionEnvironment.socketTextStream("127.0.0.1", 7777);

        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = linesDS
                .flatMap((String value, Collector<String> collector) -> {
                    String[] words = value.split(" ");
                    for (String word : words) {
                        collector.collect(word);
                    }
                })
                .startNewChain()
                .returns(Types.STRING)
                .map(m -> Tuple2.of(m, 1))
                .slotSharingGroup("slot_group_1")
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(value -> value.f0)
                .sum(1);

        sum.print();

        executionEnvironment.execute();
    }

}
