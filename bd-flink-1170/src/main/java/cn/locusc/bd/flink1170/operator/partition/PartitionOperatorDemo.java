package cn.locusc.bd.flink1170.operator.partition;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author Jay
 * 分区算子示例
 * 2023/6/15
 */
public class PartitionOperatorDemo {

    public static void main(String[] args) throws Exception {
        // StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        executionEnvironment.setParallelism(2);

        DataStreamSource<String> socketDS = executionEnvironment.socketTextStream("hadoop102", 7777);

        // shuffle随机分区: random.nextInt(下游算子并行度)
        // socketDS.shuffle().print();

        // rebalance轮询：nextChannelToSendTo = (nextChannelToSendTo + 1) % 下游算子并行度
        // 如果是 数据源倾斜的场景, source后, 调用rebalance, 就可以解决 数据源的 数据倾斜
        // socketDS.rebalance().print();

        //rescale缩放: 实现轮询,局部组队,比rebalance更高效
        // socketDS.rescale().print();

        // broadcast 广播: 发送给下游所有的子任务
        // socketDS.broadcast().print();

        // global 全局: 全部发往 第一个子任务
        // return 0;
        socketDS.global().print();

        // keyBy: 按指定key去发送, 相同key发往同一个子任务
        // socketDS.keyBy();

        // one-to-one: Forward分区器
        // socketDS.forward();

        // 总结: Flink提供了7种分区器 + 1种自定义
        executionEnvironment.execute();
    }

}
