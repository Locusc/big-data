package cn.locusc.bd.spark.scala.daios.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
 * @author Jay
 * sort算子示例
 * 2022/12/14
 */
object SortOperator {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("operator about sort")
      .master("local")
      .getOrCreate()

    val inputRDD = spark.sparkContext.parallelize(Array[(Char, Int)](
      ('D', 2), ('B', 4), ('C', 3), ('A', 5), ('B', 2), ('C', 1), ('C', 3), ('A', 4)
    ), 3)

    sortByKey(inputRDD)
    sortBy(inputRDD)
  }

  /**
   * 按照key进行排序, 就算Key一样也不会对Value进行排序
   * ascending = true标识升序
   * 按照range进行分区划分, 保证每个分区数据有序
   * 如果需要对value进行排序, 可以map转为<(Key,Value), null>
   * 也可以使用groupByKey -> mapValues(sort func)
   */
  def sortByKey(inputRDD: RDD[(Char, Int)]): Unit = {
    inputRDD.sortByKey(ascending = true, 2);
  }

  /**
   * 基于func的计算结果对rdd1中的record进行排序,
   * 和sortByKey类似, 但是不限制RDD数据类型
   * ascending = true标识升序
   * 依靠sortByKey实现, 先将数据转化 <K,V> -> <V, <K,V>>, 利用sortByKey对转化后的数据进行排序,
   * 最后只保留<V, <K,V>>中的<K,V>
   */
  def sortBy(inputRDD: RDD[(Char, Int)]): Unit = {
    inputRDD.sortBy(record => record._2, ascending = true, numPartitions = 2);
  }

}
