package cn.locusc.bd.flink.news;

/**
 * @author Jay
 * 全局配置参数
 * 2022/11/29
 */
public class GlobalConstants {

    /**
     * 数据库driver class
     */
    public static final String DRIVER_CLASS = "com.mysql.cj.jdbc.Driver";
    /**
     * 数据库jdbc url
     */
    public static final String DB_URL = "jdbc:mysql://39.107.96.199:3306/mercy?useUnicode=true&characterEncoding=utf-8&serverTimezone=Asia/Shanghai&useSSL=false";
    /**
     * 数据库user name
     */
    public static final String USER_MAME = "root";
    /**
     * 数据库password
     */
    public static final String PASSWORD = "wdnmd123";
    /**
     * 插入newscount表
     */
    public static final String INSERT_NEW_COUNT_SQL = "insert into newscount (name,count) values (?,?)";

    /**
     * 插入periodcount表
     */
    public static final String INSERT_PERIOD_COUNT_SQL = "insert into periodcount (logtime,count) values (?,?)";

}
