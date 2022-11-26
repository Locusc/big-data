package cn.locusc.spark.scala.basic

import org.apache.spark.{SparkConf, SparkContext}

object ScalaWordCountObject {

  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      System.err.println("Usage:ScalaWordCount <input> <output>")
      System.exit(1)
    }

    // 输入路径
    val input = args(0)
    //输出路径
    val output = args(1)

    val conf = new SparkConf().setAppName("ScalaWordCount")

    val sc = new SparkContext(conf)

    // 读取数据
    val lines = sc.textFile(input)

    val resRDD = lines.flatMap(_.split("\\s+")).map((_, 1)).reduceByKey(_ + _)

    // 保存结果
    resRDD.saveAsTextFile(output)

    sc.stop()
  }

}
