package cn.locusc.bd.flink.news.datastream;

import cn.locusc.bd.flink.news.GlobalConstants;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * @author Jay
 * flume -> kafka -> flink -> mysql
 * 2022/11/29
 */
public class MysqlDataSink extends RichSinkFunction<Tuple2<String, Integer>> implements Serializable {

    private Connection connection;

    private PreparedStatement preparedStatement;

    private final MapFunction<Tuple2<String, Integer>, Tuple3<String, String, String>> func;

    public MysqlDataSink(MapFunction<Tuple2<String, Integer>, Tuple3<String, String, String>> func) {
        this.func = func;
    }

    @Override
    public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
        super.open(parameters);
        // 加载JDBC驱动
        Class.forName(GlobalConstants.DRIVER_CLASS);
        // 获取数据库连接
        connection = DriverManager.getConnection(GlobalConstants.DB_URL, GlobalConstants.USER_MAME, GlobalConstants.PASSWORD);
    }

    @Override
    public void close() throws Exception {
        super.close();
        if(preparedStatement != null){
            preparedStatement.close();
        }
        if(connection != null){
            connection.close();
        }
        super.close();
    }

    @Override
    public void invoke(Tuple2<String, Integer> value, Context context) {
        try {
            Tuple3<String, String, String> apply = func.map(value);
            // 查询数据
            preparedStatement = connection.prepareStatement(apply.f0);
            ResultSet resultSet = preparedStatement.executeQuery();
            if(resultSet.next()){
                // 更新数据
                preparedStatement.executeUpdate(apply.f1);
            }else{
                // 插入数据
                preparedStatement.execute(apply.f2);
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }

}
