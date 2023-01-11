package cn.locusc.bd.spark.scala.daios.operator.action

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
 * @author Jay
 * foreach算子示例
 * 和map以及mapPartition相似, 但是不生成新的RDD
 * 2022/12/24
 */
object ForeachOperator {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("operator about count")
      .master("local[2]")
      .getOrCreate()

    val inputRDD1 = spark.sparkContext.parallelize(Array[(Int, Char)](
      (3, 'c'), (3, 'f'), (5, 'e'), (4, 'd'), (1, 'h'), (2, 'b'), (5, 'e'), (2, 'g')
    ), 3)
  }

  /**
   * 将RDD中的每个record按照func进行处理
   */
  def foreach(inputRDD1: RDD[(Int, Char)]): Unit ={
    // 输出每个record的value
    inputRDD1.foreach(f => println(f._2))
  }

  /**
   * 将RDD中的每个分区中的record按照func进行处理
   */
  def foreachPartition(inputRDD1: RDD[(Int, Char)]): Unit = {
    // 只输出Key值大于或等于3的record
    inputRDD1.foreachPartition(fp => {
      while (fp.hasNext) {
        val record = fp.next()
        if (record._1 >= 3) {
          println(record)
        }
      }
    })
  }

}
