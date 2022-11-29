package cn.locusc.bd.spark.scala.news.streaming

/**
 * @author Jay
 * 基础常量
 * 2022/11/25
 */
object BasicConstants {

  var driver:String = "com.mysql.jdbc.Driver"

  var url:String = "jdbc:mysql://39.107.96.199:3306/mercy?useUnicode=true&characterEncoding=utf-8&serverTimezone=Asia/Shanghai&useSSL=false"

  var userName:String = "root"

  var passWord:String = "wdnmd123"

  var kafkaServer:String = "192.168.1.7:9092,192.168.1.8:9092,192.168.1.9:9092"

  var topic:String = "sogoulogs"

}
