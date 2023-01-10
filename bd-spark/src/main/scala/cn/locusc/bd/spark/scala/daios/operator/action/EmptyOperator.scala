package cn.locusc.bd.spark.scala.daios.operator.action

import cn.locusc.bd.spark.scala.daios.operator.action.CutOffOperator.take
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
 * @author Jay
 * empty算子示例
 * 2022/12/24
 */
object EmptyOperator {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("operator about empty")
      .master("local[2]")
      .getOrCreate()

    val inputRDD = spark.sparkContext.parallelize(Array[String](
      "a", "b", "c", "d", "e", "f", "g", "h", "i"
    ), 3)

    empty(inputRDD)
  }

  /**
   * 判断RDD是否为空, 如果RDD不包含任何record, 那么返回true
   * 提前判断RDD为空, 可以避免提交冗余的job
   */
  def empty(inputRDD: RDD[String]): Unit = {
    val isEmpty = inputRDD.isEmpty()
  }

}
