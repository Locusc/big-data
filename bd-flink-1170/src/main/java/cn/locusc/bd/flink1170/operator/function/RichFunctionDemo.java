package cn.locusc.bd.flink1170.operator.function;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author Jay
 * 富函数示例
 * 1、多了生命周期管理方法：
 *    open(): 每个子任务, 在启动时, 调用一次
 *    close(): 每个子任务, 在结束时, 调用一次
 *      => 如果是flink程序异常挂掉, 不会调用close
 *      => 如果是正常调用cancel命令, 可以close
 * 2、多了一个运行时上下文
 *    可以获取一些运行时的环境信息, 比如 子任务编号、名称、其他的.....
 * 2023/6/15
 */
public class RichFunctionDemo {

    public static void main(String[] args) throws Exception {
        // StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        executionEnvironment.setParallelism(2);

        // DataStreamSource<Integer> source = env.fromElements(1, 2, 3, 4);

        DataStreamSource<String> source = executionEnvironment.socketTextStream("hadoop102", 7777);

        SingleOutputStreamOperator<Integer> singleOutputStreamOperator = source.map(new RichMapFunction<String, Integer>() {

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                System.out.println(
                        "子任务编号=" + getRuntimeContext().getIndexOfThisSubtask()
                                + "，子任务名称=" + getRuntimeContext().getTaskNameWithSubtasks()
                                + ",调用open()");
            }

            @Override
            public void close() throws Exception {
                super.close();
                System.out.println(
                        "子任务编号=" + getRuntimeContext().getIndexOfThisSubtask()
                                + "，子任务名称=" + getRuntimeContext().getTaskNameWithSubtasks()
                                + ",调用close()");
            }

            @Override
            public Integer map(String value) throws Exception {
                return Integer.parseInt(value) + 1;
            }
        });


        singleOutputStreamOperator.print();

        executionEnvironment.execute();
    }

}
