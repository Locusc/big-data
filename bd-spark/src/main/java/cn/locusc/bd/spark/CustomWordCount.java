package cn.locusc.bd.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

/**
 * @author Jay
 * spark java版本workcount
 * 2022/11/24
 */
public class CustomWordCount {

    public static void main(String[] args) {
        // 参数检查
        if(args.length < 2 ){
            System.err.println("Usage:CustomWordCount input output");
            System.exit(1);
        }

        // 输入路径
        String inputPath = args[0];
        // 输出路径
        String outputPath = args[1];

        // 创建SparkContext
        SparkConf conf = new SparkConf().setAppName("CustomWordCount");
        JavaSparkContext jsc = new JavaSparkContext(conf);

        // 读取数据
        JavaRDD<String> inputRDD = jsc.textFile(inputPath);

        // 纯函数操作演示
        // inputRDD.flatMap(fm -> Arrays.asList(fm.split("\\s+")).iterator())
        //         .mapToPair(mtp -> new Tuple2<>(mtp, 1))
        //         .reduceByKey(Integer::sum);

        //flatmap扁平化操作
        JavaRDD<String> words = inputRDD.flatMap(new FlatMapFunction<String, String>() {

            private static final long serialVersionUID = 1L;

            @Override
            public Iterator<String> call(String line) throws Exception {
                return Arrays.asList(line.split("\\s+")).iterator();
            }
        });

        //map 操作
        JavaPairRDD<String, Integer> pairRDD = words.mapToPair(new PairFunction<String, String, Integer>() {

            private static final long serialVersionUID = 1L;

            @Override
            public Tuple2<String, Integer> call(String word) throws Exception {
                return new Tuple2<String, Integer>(word,1);
            }
        });

        //reduce操作
        JavaPairRDD<String, Integer> result = pairRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {

            private static final long serialVersionUID = 1L;

            @Override
            public Integer call(Integer x, Integer y) throws Exception {
                return x+y;
            }
        });

        //保存结果
        result.saveAsTextFile(outputPath);

        //关闭jsc
        jsc.close();
    }

}
