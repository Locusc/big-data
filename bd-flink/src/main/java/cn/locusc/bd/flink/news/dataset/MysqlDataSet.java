package cn.locusc.bd.flink.news.dataset;

import cn.locusc.bd.flink.news.GlobalConstants;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.types.Row;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.jdbc.JDBCOutputFormat;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * @author Jay
 * local file -> flink -> mysql
 * 2022/11/29
 */
public class MysqlDataSet {

    public static void main(String[] args) throws Exception {
        // 获取Flink的运行环境
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 读取数据
        DataSet<String> ds = env.readTextFile(args[0]);

        // 对数据进行过滤
        DataSet<String> filter = ds.filter((value) -> value.split(",").length == 6);

        // 统计所有新闻话题浏览量
        DataSet<Tuple2<String, Integer>> newsCounts = filter
                .flatMap((FlatMapFunction<String, Tuple2<String, Integer>>) (in, out) -> {
                    String[] tokens = in.toLowerCase().split(",");
                    out.collect(new Tuple2<>(tokens[2], 1));
                })
                .returns(TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {}))
                .groupBy(0)
                .sum(1);

        // 数据转换
        DataSet<Row> outputData =newsCounts.map((MapFunction<Tuple2<String, Integer>, Row>) t -> {
            Row row = new Row(2);
            row.setField(0, t.f0.replaceAll("[\\[\\]]", ""));
            row.setField(1, t.f1);
            return row;
        });

        // 数据输出到newscount表
        outputData.output(JDBCOutputFormat.buildJDBCOutputFormat()
                .setDrivername(GlobalConstants.DRIVER_CLASS)
                .setDBUrl(GlobalConstants.DB_URL)
                .setUsername(GlobalConstants.USER_MAME)
                .setPassword(GlobalConstants.PASSWORD)
                .setQuery(GlobalConstants.INSERT_NEW_COUNT_SQL)
                .finish());

        // 统计所有时段新闻浏览量
        DataSet<Tuple2<String, Integer>> periodCounts = filter
                .flatMap((FlatMapFunction<String, Tuple2<String, Integer>>) (in, out) -> {
                    String[] tokens = in.toLowerCase().split(",");
                    out.collect(new Tuple2<>(tokens[0], 1));
                })
                .returns(TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {}))
                .groupBy(0)
                .sum(1);

        // 数据转换
        DataSet<Row> outputData2 = periodCounts.map((MapFunction<Tuple2<String, Integer>, Row>) t -> {
            Row row = new Row(2);
            row.setField(0, t.f0);
            row.setField(1, t.f1);
            return row;
        });

        // 数据输出到periodcount表
        outputData2.output(JDBCOutputFormat.buildJDBCOutputFormat()
                .setDrivername(GlobalConstants.DRIVER_CLASS)
                .setDBUrl(GlobalConstants.DB_URL)
                .setUsername(GlobalConstants.USER_MAME)
                .setPassword(GlobalConstants.PASSWORD)
                .setQuery(GlobalConstants.INSERT_PERIOD_COUNT_SQL)
                .finish());

        // 执行flink 程序
        env.execute("FlinkDataSetDemo");
    }

}
