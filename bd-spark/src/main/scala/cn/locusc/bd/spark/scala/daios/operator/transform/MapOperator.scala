package cn.locusc.bd.spark.scala.daios.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
 * @author Jay
 * map算子示例
 * 2022/12/10
 */
object MapOperator {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("operator about map")
      .master("local")
      .getOrCreate()

    // 源数据是一个被划分为3份的<K, V>数组
    val inputRDD = spark.sparkContext.parallelize(Array[(Int, Char)](
      (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (2, 'e'), (3, 'f'), (2, 'g'), (1, 'h')
    ), 3)
    map(inputRDD)

    // 数据源是3个字符串
    val flatMapRDD = spark.sparkContext.parallelize(Array[String](
      "how do you do", "are you ok", "thanks", "bye bye", "i'm ok"
    ), 3)
    flatMap(flatMapRDD)

    val flatMapValueRDD = spark.sparkContext.parallelize(Array[(Int, String)](
      (1, "how do you do"), (2, "are you ok"), (3, "thanks"), (4, "bye bye"), (5, "i'm ok")
    ), 3)
    flatMapValue(flatMapValueRDD)

    val mapPartitionsRDD = spark.sparkContext.parallelize(List(
      1, 2, 3, 4, 5, 6, 7, 8, 9
    ), 3)
    mapPartitions(mapPartitionsRDD)
  }

  /**
   * 1-1 MapPartitionsRDD
   */
  def map(inputRDD: RDD[(Int, Char)]): Unit = {
    // 对于每个record, 如m = (1, 'a'), 使用m._1得到其Key值, 加上下划线
    // 然后使用m_2加上其Value值
    val resultRDD = inputRDD.map(m => m._1 + "_" + m._2)

    // 输出RDD包含的record
    resultRDD.foreach(println)
  }

  /**
   * 1-1 MapPartitionsRDD
   */
  def mapValues(inputRDD: RDD[(Int, Char)]): Unit = {
    // 对于每个record, 如m = (1, 'a'), 在其Value上值后加上"_1"
    val resultRDD = inputRDD.mapValues(mv => mv + "_1")

    // 输出RDD包含的record
    resultRDD.foreach(println)
  }

  /**
   * 1-1 MapPartitionsRDD
   * 使用flatMap()对字符串进行分词, 得到一组单词
   */
  def flatMap(inputRDD: RDD[String]): Unit = {
    inputRDD.flatMap(fm => fm.split(" "))
  }

  /**
   * 1-1 MapPartitionsRDD
   */
  def flatMapValue(inputRDD: RDD[(Int, String)]): Unit = {
    inputRDD.flatMapValues(fmv => fmv.split(" "))
  }

  /**
   * 计算每个分区中奇数的和与偶数的和
   * 可以使用数据结构持有中间结果, 比如在mapPartitions中建立数据库连接, 然后新的数据iter.next()转化成数据表中的一行
   * 避免像map对每一个record操作, 造成数据库重复连接
   * 1-1 MapPartitionsRDD
   */
  def mapPartitions(inputRDD: RDD[Int]): Unit = {
    inputRDD.mapPartitions(mp => {
      var result = List[Int]()
      var odd = 0
      var even = 0

      while (mp.hasNext) {
        val Value = mp.next()
        if (Value % 2 == 0) {
          even += Value // 计算偶数的和
        } else {
          odd += Value // 计算奇数的和
        }
      }

      result = result :+ odd :+ even // 将计算结果放入result列表中
      result.iterator
    })
  }

  /**
   * mapPartitionsWithIndex 增加了分区的索引
   * 1-1 MapPartitionsRDD
   */
  def mapPartitionsWithIndex(inputRDD: RDD[Int]): Unit = {
    val resultRDD = inputRDD.mapPartitionsWithIndex((pid, iter) => {
      var result = List[String]()
      var odd = 0
      var even = 0

      while (iter.hasNext) {
        val Value = iter.next()
        if (Value % 2 == 0) {
          even += Value // 计算偶数的和
        } else {
          odd += Value // 计算奇数的和
        }
      }

      // 将(pid, odd) 存放到List中
      result = result :+ "pid = " + pid + ", odd = " + odd
      // 将(pid, even) 存放到List中
      result = result :+ "pid = " + pid + ", even = " + even
      result.iterator // 返回List
    })

    resultRDD.mapPartitionsWithIndex((pid, iter) => {
      // 输出每个record的partition id和value
      iter.map(Value => "Pid: " + pid + ", Value: " + Value)
    }).foreach(println)
  }

}
