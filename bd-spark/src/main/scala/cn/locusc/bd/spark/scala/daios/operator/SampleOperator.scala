package cn.locusc.bd.spark.scala.daios.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
 * @author Jay
 * Sample算子示例(抽样)
 * 有放回(伯努利抽样模型), 无放回(泊松分布)与概率论中使用的模型有关
 * 这里可以简单理解为是否可以重复抽取
 * 2022/12/10
 */
object SampleOperator {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("operator about sample")
      .master("local")
      .getOrCreate()

    val inputRDD = spark.sparkContext.parallelize(List(
      (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (2, 'e'), (3, 'f'), (2, 'g'), (1, 'h')
    ), 3)

    sample(inputRDD)
  }

  /**
   * 1-1 PartitionwiseSampledRDD or MapPartitionsRDD (可能是书中有勘误, 但是都是1对1的数据关系)
   */
  def sample(inputRDD: RDD[(Int, Char)]): Unit = {
    // 使用无放回模式, 从inputRDD的数据中抽取50%的数据
    inputRDD.sample(withReplacement = false, fraction = 0.5)
    // 使用无放回模式, 从inputRDD的数据中抽取50%的数据
    inputRDD.sample(withReplacement = true, fraction = 0.5)
  }

  /**
   * 1-1 MapPartitionsRDD
   */
  def sampleByKey(inputRDD: RDD[(Int, Char)]): Unit = {
    // 使用无放回模式, 从inputRDD的数据中抽取50%的数据
    val map = Map((1 -> 0.8), (2  -> 0.5))
    inputRDD.sampleByKey(withReplacement = false, fractions = map)
    // 使用无放回模式, 从inputRDD的数据中抽取50%的数据
    inputRDD.sampleByKey(withReplacement = true, fractions = map)
  }

}

