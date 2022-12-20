package cn.locusc.bd.spark.scala.daios.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
 * @author Jay
 * Filter算子示例
 * 2022/12/10
 */
object FilterOperator {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("operator about filter")
      .master("local")
      .getOrCreate()

    // 源数据是一个被划分为3份的<K, V>数组
    val inputRDD = spark.sparkContext.parallelize(Array[(Int, Char)](
      (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (2, 'e'), (3, 'f'), (2, 'g'), (1, 'h')
    ), 3)

    filter(inputRDD)
  }

  /**
   * 输出rdd1中Key为偶数的record
   * 1-1 MapPartitionsRDD
   */
  def filter(inputRDD: RDD[(Int, Char)]): Unit = {
    val resultRDD = inputRDD.filter(f => f._1 % 2 == 0)

    // 输出RDD包含的record
    resultRDD.foreach(println)
  }

  /**
   * 输出rdd1中Key在[2,4]中的偶数
   * 1-1 MapPartitionsRDD
   */
  def filterByRange(inputRDD: RDD[(Int, Char)]): Unit = {
    val resultRDD = inputRDD.filterByRange(2, 4)

    // 输出RDD包含的record
    resultRDD.foreach(println)
  }

}
