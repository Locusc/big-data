package cn.locusc.bd.spark.scala.news.streaming

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery, Trigger}

object SparkStructuredStreaming {

  def newsSqlStr(record: Row): (String, String, String) = {
    // write string to connection
    val name = record.getAs[String]("name").replaceAll("[\\[\\]]", "")
    val count = record.getAs[Long]("count").asInstanceOf[Int]
    val sql = "select 1 from newscount where name = '"+name+"'"

    val insertSql = "insert into newscount(name,count) values('"+name+"',"+count+")"

    val updateSql = "update newscount set count = "+count+" where name = '"+name+"'"

    (sql, insertSql, updateSql)
  }

  def periodSqlStr(record: Row): (String, String, String) = {
    // write string to connection
    val logtime = record.getAs[String]("logtime")
    val count = record.getAs[Long]("count").asInstanceOf[Int]
    val sql = "select 1 from periodcount where logtime = '"+logtime+"'"

    val insertSql = "insert into periodcount(logtime,count) values('"+logtime+"',"+count+")"

    val updateSql = "update periodcount set count = "+count+" where logtime = '"+logtime+"'"

    (sql, insertSql, updateSql)
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("sogoulogs")
      .master("local[2]")
      .getOrCreate()

    import spark.implicits._
    import org.apache.spark.sql.functions._
    //读取数据
    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", BasicConstants.kafkaServer)
      .option("subscribe", BasicConstants.topic)
      .load()

    // 进行词频统计
//    val resultStreamDF: DataFrame = df
//      // 获取value字段的值，转换为String类型
//      .selectExpr("CAST(value AS STRING)")
//      // 转换为Dataset类型
//      .as[String]
//      // 过滤数据
//      .filter(line => null != line && line.trim.length > 0)
//      // 分割单词
//      .flatMap(line => line.trim.split("\\s+"))
//      // 按照单词分组，聚合
//      .groupBy($"value").count()
//    // 设置Streaming应用输出及启动
//    val query: StreamingQuery = resultStreamDF.writeStream
//      .outputMode(OutputMode.Complete())
//      .format("console").option("numRows", "10").option("truncate", "false")
//      .start()
//    query.awaitTermination() // 查询器等待流式应用终止
//    query.stop() // 等待所有任务运行完成才停止运行

    println("-----------------------------------------------------------------")
    val ds= df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .as[(String, String)]
    val filter = ds.map(x => {
      println("x._2")
      x._2
    }).map(_.split(",")).filter(_.length==6)

    // 统计所有新闻话题浏览量
    // 这里能够转换是因为spark sql里面以及有一张sogoulogs的表, 可以直接转换DataFrame
    val newsCounts = filter.map(x =>x(2)).groupBy("value").count().toDF("name","count")
    // 数据输出
    val writer = new JDBCSink(BasicConstants.url,BasicConstants.userName,BasicConstants.passWord, newsSqlStr)
    val query = newsCounts.writeStream
      .foreach(writer)
      .outputMode("update")
      .trigger(Trigger.ProcessingTime("2 seconds"))
      .start()

    // 统计每个时段新闻话题浏览量
    val periodCounts = filter.map(x =>x(0)).groupBy("value").count().toDF("logtime","count")
    val writer2 = new JDBCSink(BasicConstants.url,BasicConstants.userName,BasicConstants.passWord, periodSqlStr)
    val query2 = periodCounts.writeStream
      .foreach(writer2)
      .outputMode("update")
      .trigger(Trigger.ProcessingTime("2 seconds"))
      .start()

    query.awaitTermination()
    query2.awaitTermination()
  }

}
