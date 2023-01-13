package cn.locusc.bd.spark.scala.daios.operator.action

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
 * @author Jay
 * lookUp算子示例
 * 2022/12/24
 */
object LookUpOperator {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("operator about lookUp")
      .master("local[2]")
      .getOrCreate()

    val inputRDD = spark.sparkContext.parallelize(Array[(Int, Char)](
      (3, 'c'), (3, 'f'), (5, 'e'), (4, 'd'), (1, 'h'), (2, 'b'), (5, 'e'), (2, 'g')
    ), 3)

    lookUp(inputRDD)
  }

  /**
   * 找出RDD中包含特定Key的Value, 将这些Value形成List
   */
  def lookUp(inputRDD: RDD[(Int, Char)]): Unit = {
    val chars = inputRDD.lookup(3);
  }

}
