package cn.locusc.bd.spark.scala.daios.operator.action

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
 * @author Jay
 * collect算子示例
 * 2022/12/24
 */
object CollectOperator {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("operator about collect")
      .master("local[2]")
      .getOrCreate()

    val inputRDD1 = spark.sparkContext.parallelize(Array[(Int, Char)](
      (3, 'c'), (3, 'f'), (5, 'e'), (4, 'd'), (1, 'h'), (2, 'b'), (5, 'e'), (2, 'g')
    ), 3)
  }

  /**
   * 将RDD中的record收集到Driver端
   */
  def collect(inputRDD1: RDD[(Int, Char)]): Unit = {
    inputRDD1.collect()
  }

  /**
   * 将RDD中的<K,V> record收集到Driver端, 得到<K,V> Map
   */
  def collectAsMap(inputRDD1: RDD[(Int, Char)]): Unit = {
    inputRDD1.collectAsMap()
  }

}
