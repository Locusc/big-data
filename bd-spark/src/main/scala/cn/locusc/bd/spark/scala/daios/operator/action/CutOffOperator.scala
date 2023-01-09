package cn.locusc.bd.spark.scala.daios.operator.action

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
 * @author Jay
 * 截断算子示例
 * 2022/12/24
 */
object CutOffOperator {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("operator about cut off")
      .master("local[2]")
      .getOrCreate()

    val inputRDD1 = spark.sparkContext.parallelize(Array[String](
      "a", "b", "c", "d", "e", "f", "g", "h", "i"
    ), 3)

    take(inputRDD1)
  }

  /**
   * 将RDD中前num个record取出, 形成一个数组
   * 先从第一个分区中取出, 如果num大于第一个分区中的record数量再从后面的分区中继续取
   * 为了提高效率, spark在第一个分区中取record的时候会估计还需要对多少个后续的分区进行操作
   */
  def take(inputRDD: RDD[String]): Unit = {
    inputRDD.take(3);
  }

  /**
   * 只取出RDD中第一个 record, 等价于take(1)
   */
  def first(inputRDD: RDD[String]): Unit = {
    inputRDD.first();
  }

  /**
   * 取出RDD中最小的num个record, 要求RDD中的record是可比较的
   * 首先使用map在每个分区中寻找最小的num个record,
   * 然后将这些record收集到Driver端, 进行排序, 然后取出前num个record
   */
  def takeOrdered(inputRDD: RDD[String]): Unit = {
    inputRDD.takeOrdered(2);
  }

  /**
   * 取出RDD中最大的num个record, 要求RDD中的record是可比较的
   * 前四个操作都需要将数据收集到Driver端, 因此不适合num较大的情况
   */
  def top(inputRDD: RDD[String]): Unit = {
    inputRDD.top(2);
  }

  /**
   * 计算RDD中record的最大值
   */
  def max(inputRDD: RDD[String]): Unit = {
    inputRDD.max();
  }

  /**
   * 计算RDD中record的最小值
   * max和min都是基于reduce(func)实现, func的语义是选择最大值或最小值
   */
  def min(inputRDD: RDD[String]): Unit = {
    inputRDD.min();
  }

}
