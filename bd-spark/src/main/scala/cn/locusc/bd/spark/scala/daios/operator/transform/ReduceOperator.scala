package cn.locusc.bd.spark.scala.daios.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
 * @author Jay
 * reduce算子示例
 * 2022/12/13
 */
object ReduceOperator {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("operator about group")
      .master("local")
      .getOrCreate()

    val inputRDD = spark.sparkContext.parallelize(Array[(Int, String)](
      (1, "a"), (2, "b"), (3, "c"), (4, "d"), (2, "e"), (3, "f"), (2, "g"), (1, "h")
    ), 3)
    combineByKey(inputRDD)
  }

  /**
   * reduceByKey比groupByKey在Shuffle前多了一个本地combine操作,
   * shuffle对key进行hash划分, 所以不能是太复杂的类型
   */
  def reduceByKey(inputRDD: RDD[(Int, String)]): Unit = {
    inputRDD.reduceByKey((x, y) => x + "_" + y, 2)
  }

  /**
   * reduceByKey可以当作特殊版的aggregateByKey
   * 当seqOp处理中间数据很大, 出现shuffle spill, spark会在map端进行combOp
   * 将磁盘上经过处理的<K,V‘>record与内存中经过seqOp处理的<K,V'>record进行融合
   */
  def aggregateByKey(inputRDD: RDD[(Int, String)]): Unit = {
    inputRDD.aggregateByKey("x", 2)(
      seqOp = {
        _ + "_" + _
      },
      combOp = {
        _ + "@" + _
      }
    )
  }

  /**
   * reduceByKey和aggregateByKey都是利用combineByKey实现的
   * 与aggregateByKey不同的是, combineByKey的初始化是一个函数
   * createCombiner只作用于相同key的第一个record
   */
  def combineByKey(inputRDD: RDD[(Int, String)]): Unit = {
    inputRDD.mapValues(mv => mv.head)
      .combineByKey((V: Char) => {
        if (V == 'c') {
          V + "0"
        } else {
          V + "1"
        }
      },
        (c: String, V: Char) => c + "+" + V,
        (c1: String, c2: String) => c1 + "_" + c2,
        2
      )
      .mapPartitionsWithIndex((pid, iter) => {
        iter.map(Value => "PID: " + pid + ", Value: " + Value)
      })
      .foreach(println)
  }

  /**
   * 简化版的aggregateByKey seqOp和combOp共用一个func
   * 相比reduceByKey多了一个zeroValue
   */
  def foldByKey(inputRDD: RDD[(Int, String)]): Unit = {
    inputRDD.foldByKey("x", 2)(
      func = {
        _ + "_" + _
      }
    )
  }

}
