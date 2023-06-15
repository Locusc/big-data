package cn.locusc.bd.flink1170.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author Jay
 * 文件数据源示例
 * 2023/6/14
 */
public class FileSourceDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

        executionEnvironment.setParallelism(1);

        // 旧的DataSet Api
        // executionEnvironment.readTextFile();

        // 新的1.16.0版本以后的批流一体 DataStream Api
        FileSource<String> fileSource = FileSource
                .forRecordStreamFormat(
                    new TextLineInputFormat(),
                    new Path("input/word.txt")
                )
                .build();

        executionEnvironment.fromSource(fileSource, WatermarkStrategy.noWatermarks(), "file-source")
                .print();

        executionEnvironment.execute();
    }

}
