package cn.locusc.bd.flink1170.preview;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @author Jay
 * flink1.17.0版本的wordcount(DataSet)
 * 2023/6/12
 */
public class WordCountDataSet {

    public static void main(String[] args) throws Exception {
        // 创建执行环境 getExecutionEnvironment可以自动判断环境
        ExecutionEnvironment executionEnvironment = ExecutionEnvironment.getExecutionEnvironment();
        // 读取数据 从文件中读取数据
        DataSource<String> lineDS = executionEnvironment.readTextFile("input/word.txt");
        // 切分 转换
        FlatMapOperator<String, Tuple2<String, Integer>> wordAndOne = lineDS.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> collector) throws Exception {
                // 按照空格切分单词
                String[] words = value.split(" ");
                // 将单词转换为元组 (word,1)
                for (String word : words) {
                    // 使用Collector向下游发送数据
                    collector.collect(new Tuple2<>(word, 1));
                }
            }
        });

        // 按照word进行分组
        UnsortedGrouping<Tuple2<String, Integer>> wordAndOneGroupBy = wordAndOne.groupBy(0);

        // 各个分组内的数据进行聚合 1为位置 表示元组的第一个元素
        AggregateOperator<Tuple2<String, Integer>> sum = wordAndOneGroupBy.sum(1);

        sum.print();
    }

}
