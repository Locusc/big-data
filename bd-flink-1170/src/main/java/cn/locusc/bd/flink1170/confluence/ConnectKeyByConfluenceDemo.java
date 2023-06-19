package cn.locusc.bd.flink1170.confluence;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Jay
 * Connect以及KeyBy合流示例
 * 实现互相匹配的效果:  两条流, 不一定谁的数据先来
 *  1、每条流, 有数据来, 存到一个变量中
 *      hashMap
 *      => key=id,第一个字段值
 *      => value=List<数据>
 *  2、每条流有数据来的时候, 除了存变量中, 不知道对方是否有匹配的数据, 要去另一条流存的变量中 查找是否有匹配上的
 * 2023/6/15
 */
public class ConnectKeyByConfluenceDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

        executionEnvironment.setParallelism(2);

        DataStreamSource<Tuple2<Integer, String>> source1 = executionEnvironment.fromElements(
                Tuple2.of(1, "a1"),
                Tuple2.of(1, "a2"),
                Tuple2.of(2, "b"),
                Tuple2.of(3, "c")
        );

        DataStreamSource<Tuple3<Integer, String, Integer>> source2 = executionEnvironment.fromElements(
                Tuple3.of(1, "aa1", 1),
                Tuple3.of(1, "aa2", 2),
                Tuple3.of(2, "bb", 1),
                Tuple3.of(3, "cc", 1)
        );

        ConnectedStreams<Tuple2<Integer, String>, Tuple3<Integer, String, Integer>> connect = source1.connect(source2);

        // 多并行度下, 需要根据关联条件进行keyBy, 才能保证key相同的数据到一起去, 才能匹配上
        ConnectedStreams<Tuple2<Integer, String>, Tuple3<Integer, String, Integer>> connectKeyBy = connect.keyBy(k1 -> k1.f0, k2 -> k2.f0);

        SingleOutputStreamOperator<String> process = connectKeyBy.process(new CoProcessFunction<Tuple2<Integer, String>, Tuple3<Integer, String, Integer>, String>() {

            final Map<Integer, List<Tuple2<Integer, String>>> s1Cache = new HashMap<>();

            final Map<Integer, List<Tuple3<Integer, String, Integer>>> s2Cache = new HashMap<>();

            /**
             * 第一条流的处理逻辑
             * @param value 第一条流的数据
             * @param ctx   上下文
             * @param out   采集器
             */
            @Override
            public void processElement1(Tuple2<Integer, String> value, CoProcessFunction<Tuple2<Integer, String>, Tuple3<Integer, String, Integer>, String>.Context ctx, Collector<String> out) throws Exception {
                Integer id = value.f0;
                // 1 s1的数据来了, 就存到变量中
                if (!s1Cache.containsKey(id)) {
                    // 1.1 如果key不存在, 说明是该key的第一条数据, 初始化, put进map中
                    List<Tuple2<Integer, String>> s1Values = new ArrayList<>();
                    s1Values.add(value);
                    s1Cache.put(id, s1Values);
                } else {
                    // 1.2 key存在, 不是该key的第一条数据, 直接添加到value的list中
                    s1Cache.get(id).add(value);
                }

                // 2.去s2Cache中查找是否有id能匹配上的, 匹配上就输出, 没有就不输出
                if (s2Cache.containsKey(id)) {
                    for (Tuple3<Integer, String, Integer> s2Element : s2Cache.get(id)) {
                        out.collect("s1:" + value + "<========>" + "s2:" + s2Element);
                    }
                }
            }

            /**
             * 第二条流的处理逻辑
             * @param value 第二条流的数据
             * @param ctx   上下文
             * @param out   采集器
             */
            @Override
            public void processElement2(Tuple3<Integer, String, Integer> value, CoProcessFunction<Tuple2<Integer, String>, Tuple3<Integer, String, Integer>, String>.Context ctx, Collector<String> out) throws Exception {
                Integer id = value.f0;
                // 1. s2的数据来了, 就存到变量中
                if (!s2Cache.containsKey(id)) {
                    // 1.1 如果key不存在, 说明是该key的第一条数据, 初始化, put进map中
                    List<Tuple3<Integer, String, Integer>> s2Values = new ArrayList<>();
                    s2Values.add(value);
                    s2Cache.put(id, s2Values);
                } else {
                    // 1.2 key存在, 不是该key的第一条数据, 直接添加到value的list中
                    s2Cache.get(id).add(value);
                }

                // 2. 去s1Cache中查找是否有id能匹配上的,匹配上就输出,没有就不输出
                if (s1Cache.containsKey(id)) {
                    for (Tuple2<Integer, String> s1Element : s1Cache.get(id)) {
                        out.collect("s1:" + s1Element + "<========>" + "s2:" + value);
                    }
                }
            }

        });

        process.print();

        executionEnvironment.execute();
    }

}
