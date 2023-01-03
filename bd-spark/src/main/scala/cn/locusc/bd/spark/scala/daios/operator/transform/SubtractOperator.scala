package cn.locusc.bd.spark.scala.daios.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
 * @author Jay
 * Subtract算子示例
 * 2022/12/17
 */
object SubtractOperator {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("operator about subtract")
      .master("local[2]")
      .getOrCreate()

    val inputRDD1 = spark.sparkContext.parallelize(Array[(Int, Char)](
      (3, 'c'), (3, 'f'), (5, 'e'), (4, 'd'), (1, 'h'), (2, 'b'), (5, 'e'), (2, 'g')
    ), 3)

    val inputRDD2 = spark.sparkContext.parallelize(Array[(Int, Char)](
      (1, 'A'), (2, 'B'), (3, 'C'), (2, 'D'), (6, 'E')
    ), 2)
  }

  /**
   * 计算出Key在RDD1中而不在RDD2中的record
   * 该操作首先将RDD1和RDD2中的<K,V>record按key聚合在一起, 过程类似cogroup, 得到SubtractedRDD
   * 只保留[(a), (b)]中b为()的record
   * 可以形成, OneToOneDependency和ShuffleDependency, 实现比CoGroupedRDD更高效
   */
  def subtractByKey(inputRDD1: RDD[(Int, Char)], inputRDD2: RDD[(Int, Char)]): Unit = {
    val subtractByKe = inputRDD1.subtractByKey(inputRDD2, 2)
  }

  /**
   * 计算出RDD1中而不在RDD2中的record
   * 实现原理基于subtractByKey, 适用范围更广, 针对非<K,V>类型的RDD
   * 和subtractByKey不同在于会将RDD先表示为<Key, Value> value为null, 再按照subtractByKey, 最后只保留[(a), (b)]中b为()的record
   */
  def subtract(ss: SparkSession): Unit = {
    val inputRDD1 = ss.sparkContext.makeRDD(List(1, 2, 3, 3, 4, 2, 1, 5, 6), 3)
    val inputRDD2 = ss.sparkContext.makeRDD(List(1, 1, 3, 4, 10), 2)
    val subtract = inputRDD1.subtract(inputRDD2, 2)
  }

}
