package cn.locusc.bd.spark.scala.daios.operator

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
      ('D', 2), ('B', 4), ('C', 3), ('A', 5), ('B', 2), ('C', 1), ('C', 3)
    ), 3)

    sortByKey(inputRDD)
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

}
