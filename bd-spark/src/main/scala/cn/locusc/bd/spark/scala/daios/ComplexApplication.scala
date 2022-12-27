package cn.locusc.bd.spark.scala.daios

import org.apache.spark.HashPartitioner
import org.apache.spark.sql.SparkSession

/**
 * @author Jay
 * spark物理执行计划分析
 * 2022/12/26
 */
object ComplexApplication {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("complex application")
      .master("local[2]")
      .getOrCreate()

    val sc = spark.sparkContext

    // 构建一个<K, V>类型的RDD1
    val data1 = Array[(Int, Char)]((1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e'), (3, 'f'), (2, 'g'), (1, 'h'))
    val rdd1 = sc.parallelize(data1, 3)
    // 使用HashPartitioner对rdd1进行重新划分
    val partitionedRDD = rdd1.partitionBy(new HashPartitioner(3))

    // 构建一个<K, V>类型的rdd2, 并对rdd2中record的Value进行复制
    val data2 = Array[(Int, String)]((1, "A"), (2, "B"), (3, "C"), (4, "D"))
    val rdd2 = sc.parallelize(data2, 2).map(m => (m._1, m._2 + "" + m._2))

    // 构建一个<K, V>类型的RDD3
    val data3 = Array[(Int, String)]((3, "X"), (5, "Y"), (3, "Z"), (4, "Y"))
    val rdd3 = sc.parallelize(data3, 2)

    // 将rdd2和rdd3进行union操作
    val unionRDD = rdd2.union(rdd3)

    // 将重新划分过的rdd1与unionRDD进行join操作
    val resultRDD = partitionedRDD.join(unionRDD)

    // 输出join()操作后的结果, 包括每个record及其index
    resultRDD.foreach(println)

    // Thread.sleep(999999)
  }

}
