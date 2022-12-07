package cn.locusc.bd.spark.scala.daios

import org.apache.spark.sql.SparkSession

import scala.util.Random

/**
 * @author Jay
 * GroupByTest
 * 2022/12/7
 */
object GroupByTest {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("GroupBy Test")
      .master("local")
      .getOrCreate()

    val numMappers = 3
    val numKVPairs = 4
    val valSize = 1000
    val numReducers = 2
    val input = 0 until numMappers // [0, 1, 2]

    val pairs1 = spark.sparkContext.parallelize(input, numMappers)
      .flatMap {
        _ =>
          val ranGen = new Random
          val arr1 = new Array[(Int, Array[Byte])](numKVPairs)
          for (i <- 0 until numKVPairs) {
            val byteArr = new Array[Byte](valSize)
            ranGen.nextBytes(byteArr)
            arr1(i) = (ranGen.nextInt(numKVPairs), byteArr)
          }
          arr1
      }.cache()
    // enforce that everything has been calculated and in cache

    println(pairs1.count())
    println(pairs1.toDebugString) // 打印出形成pairs1的逻辑流程图
    val results = pairs1.groupByKey(numReducers)
    println(results.count())
    println(results.toDebugString) // 打印出形成results的逻辑流程图

    spark.stop()
  }

}
