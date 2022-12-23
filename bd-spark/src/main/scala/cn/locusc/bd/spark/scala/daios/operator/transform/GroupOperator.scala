package cn.locusc.bd.spark.scala.daios.operator.transform

import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
 * @author Jay
 * group算子示例
 * 2022/12/13
 */
object GroupOperator {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("operator about group")
      .master("local")
      .getOrCreate()

    val inputRDD = spark.sparkContext.parallelize(Array[(Int, Char)](
      (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (2, 'e'), (3, 'f'), (2, 'g'), (1, 'h')
    ), 3)

    val inputRDD1 = spark.sparkContext.parallelize(Array[(Int, Char)](
      (1, 'a'), (1, 'b'), (2, 'c'), (3, 'd'), (4, 'e'), (5, 'f')
    ), 3)

    val inputRDD2 = spark.sparkContext.parallelize(Array[(Int, Char)](
      (1, 'f'), (3, 'g'), (2, 'h')
    ), 2)

    val inputRDD3 = spark.sparkContext.parallelize(Array[(Int, Char)](
      (1, 'A'), (3, 'B'), (2, 'C'), (2, 'D'), (2, 'E')
    ), 3)

    groupBy(inputRDD)
    cogroupShuffle(inputRDD1, inputRDD2)
  }

  /**
   * groupBy默认使用hash划分, 可以指定分区个数, 不指定为parentRDD分区个数
   * 并行执行, 产生大量中间数据, 占用大量内存, 一般使用reduceByKey
   */
  def groupBy(inputRDD: RDD[(Int, Char)]): Unit = {
    // inputRDD.partitioner与resultRDD的partitioner不同, 需要ShuffleDependency
    val resultRDD = inputRDD.groupByKey(2)

    // 使用Hash划分对inputRDD进行划分, 得到的inputRDD2就是59页中图3.10右图中的rdd1
    val inputRDD2 = inputRDD.partitionBy(new HashPartitioner(3))
    // inputRDD2.partitioner与resultRDD的partitioner相同, 不需要ShuffleDependency
    val resultRDD2 = inputRDD2.groupByKey(3)
  }

  /**
   * 合并多个key相同的RDD, 如果childRDD和parentRDD的分区器partitioner和分区个数相同,
   * 那么不需要shuffle混洗, 否则需要shuffle
   */
  def cogroupShuffle(inputRDD1: RDD[(Int, Char)], inputRDD2: RDD[(Int, Char)]): Unit = {
    inputRDD1.cogroup(inputRDD2, 3)
  }

}
