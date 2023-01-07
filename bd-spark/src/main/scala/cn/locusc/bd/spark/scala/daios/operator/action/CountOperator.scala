package cn.locusc.bd.spark.scala.daios.operator.action

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
 * @author Jay
 * count算子示例
 * 2022/12/24
 */
object CountOperator {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("operator about count")
      .master("local[2]")
      .getOrCreate()

    val inputRDD1 = spark.sparkContext.parallelize(Array[(Int, Char)](
      (3, 'c'), (3, 'f'), (5, 'e'), (4, 'd'), (1, 'h'), (2, 'b'), (5, 'e'), (2, 'g')
    ), 3)

    countByValue(inputRDD1)
  }

  /**
   * 统计RDD中包含的record个数 返回一个long类型
   */

  def count(inputRDD1: RDD[(Int, Char)]): Unit = {
    val long = inputRDD1.count()
  }

  /**
   * 统计RDD中每个key出现的次数(Key可能重复)
   * 返回一个Map, 要求RDD是<K,V>类型
   */
  def countByKey(inputRDD1: RDD[(Int, Char)]): Unit = {
    val intToLong = inputRDD1.countByKey()
  }

  /**
   * 统计RDD中每个record出现的次数, 返回一个Map,
   * 当数据量较大时, 这个Map会超过Driver大小, 建议使用reduceByKey对数据进行统计,
   * 最后将结果放入HDFS等
   */
  def countByValue(inputRDD1: RDD[(Int, Char)]): Unit = {
    val tupleToLong = inputRDD1.countByValue()
    println(tupleToLong)
  }


}
