package cn.locusc.bd.spark.scala.daios.operator.action

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
 * @author Jay
 *
 * 2022/12/24
 */
object SaveAsOperator {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("operator about save as")
      .master("local[2]")
      .getOrCreate()

    val inputRDD = spark.sparkContext.parallelize(Array[(Int, Char)](
      (3, 'c'), (3, 'f'), (5, 'e'), (4, 'd'), (1, 'h'), (2, 'b'), (5, 'e'), (2, 'g')
    ), 3)
  }

  /**
   * 将RDD保存为文本文件
   */
  def saveAsTextFile(inputRDD: RDD[(Int, Char)]): Unit = {
    inputRDD.saveAsTextFile("hdfs://spark/tmp/")
  }

  /**
   * 将RDD保存为序列化对象形式文件
   */
  def saveAsObjectFile(inputRDD: RDD[(Int, Char)]): Unit = {
    inputRDD.saveAsObjectFile("hdfs://spark/tmp/")
  }

  /**
   * 将RDD保存为SequenceFile形式文件, SequenceFile用于存放序列化后的对象
   */
  def saveAsSequenceFile(inputRDD: RDD[(Int, Char)]): Unit = {
    // inputRDD.saveAsSequenceFile("hdfs://spark/tmp/")
  }

  /**
   * 将RDD保存为Hadoop HDFS文件系统支持的文件
   * 以上操作都是基于saveAsHadoopFile实现
   */
  def saveAsHadoopFile(inputRDD: RDD[(Int, Char)]): Unit = {
    inputRDD.saveAsHadoopFile("hdfs://spark/tmp/")
  }

}
