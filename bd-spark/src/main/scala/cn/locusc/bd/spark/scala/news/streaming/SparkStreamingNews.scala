package cn.locusc.bd.spark.scala.news.streaming

import java.sql.{Connection, DriverManager, Statement}

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe


/**
 * @author Jay
 * SparkStreaming消费kafka中的数据
 * flume -> kafka -> spark streaming -> mysql
 * 2022/11/25
 */
object SparkStreamingNews {

  val url: String = BasicConstants.url
  val userName:String = BasicConstants.userName
  val passWord:String = BasicConstants.passWord

  /**
   * 执行sql
   * @param connection 连接信息
   * @param sols sql
   */
  def executeSql(connection: Connection, statement: Statement, sols:(String, String, String)): Unit = {
    //执行查询
    val resultSet = statement.executeQuery(sols._1)

    if(resultSet.next()){
      statement.executeUpdate(sols._2)
    }else{
      statement.execute(sols._2)
    }
  }

  /**
   * 关闭资源
   */
  def close(params:(Connection, Statement)): Unit = {
    if(params._1 != null){
      params._1.close()
    }
    if(params._2 != null){
      params._2.close()
    }
  }

  /**
   * 新闻浏览量数据插入mysql
   * @param records 执行的数据
   * @return void
   */
  def topicViews(records: Iterator[(String, Int)]): Unit = {

    var conn: Connection = null
    var statement:Statement = null

    try {
      conn = DriverManager.getConnection(url, userName, passWord)

      records.foreach(f => {
        val name = f._1.replaceAll("[\\[\\]]", "")
        val count = f._2

        println(name + "-------------------->" + count)

        val sql = "select 1 from newscount "+" where name = '"+name+"'"

        val updateSql = "update newscount set count = count+"+count+" where name ='"+name+"'"

        val insertSql = "insert into newscount(name,count) values('"+name+"',"+count+")"

        //实例化statement对象
        statement = conn.createStatement()

        executeSql(conn, statement, (sql, insertSql, updateSql))
      })
    } catch {
      case e:Exception => e.printStackTrace()
    } finally {
      close(conn, statement)
    }
  }

  def timeInterval(records:Iterator[(String, Int)]): Unit = {
    var conn:Connection = null
    var statement:Statement = null

    try{
      conn = DriverManager.getConnection(url, userName, passWord)

      records.foreach(t => {

        val logtime = t._1
        val count = t._2
        println(logtime + "------------------->" + count)

        val sql = "select 1 from periodcount "+" where logtime = '"+logtime+"'"

        val updateSql = "update periodcount set count = count+"+count+" where logtime ='"+logtime+"'"

        val insertSql = "insert into periodcount(logtime,count) values('"+logtime+"',"+count+")"

        statement = conn.createStatement()

        executeSql(conn, statement, (sql, insertSql, updateSql))
      })
    }catch{
      case e:Exception => e.printStackTrace()
    }finally{
      close(conn, statement)
    }
  }

  def foreach(records:(String, Int)): Unit = {
    print(records._1)
    print(records._2)
  }

  def main(args: Array[String]): Unit = {
    // Create the context with a 1 second batch size
    val sparkConf = new SparkConf().setAppName("sogoulogs").setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, Seconds(1))

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "192.168.1.7:9092,192.168.1.8:9092,192.168.1.9:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "sogoulogs",
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (true: java.lang.Boolean)
    )

    val topics = Array("sogoulogs")
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )

    val lines = stream.map(record =>  record.value)

    //无效数据过滤
    val filter = lines.map(_.split(",")).filter(_.length==6)

    //统计所有新闻话题浏览量
    val newsCounts = filter.map(x => (x(2), 1)).reduceByKey(_ + _)
    newsCounts.foreachRDD(rdd => {
      print("--------------------------------------------")
      //分区并行执行
      rdd.foreachPartition(topicViews)
    })
    newsCounts.print()

    //统计所有时段新闻浏览量
    val periodCounts = filter.map(x => (x(0), 1)).reduceByKey(_ + _)
    periodCounts.foreachRDD(rdd =>{
      rdd.foreachPartition(timeInterval)
    })
    periodCounts.print()

    ssc.start()
    ssc.awaitTermination()
  }

}
