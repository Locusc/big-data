package cn.locusc.bd.spark.scala.daios.operator

import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
 * @author Jay
 * join算子示例
 * 2022/12/15
 */
object JoinOperator {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("operator about join")
      .master("local")
      .getOrCreate()

    val inputRDD1 = spark.sparkContext.parallelize(Array[(Int, Char)](
      (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd')
    ), 2)

    val inputRDD2 = spark.sparkContext.parallelize(Array[(Int, Char)](
      (1, 'A'), (2, 'B')
    ), 2)

  }

  /**
   * 不需压迫shuffle
   * 最多支持四个RDD同时进行cogroup
   * 实际生成两个RDD
   *  CoGroupedRDD将数据聚合在一起
   *  MapPartitionsRDD将数据转化为CompactBuffer(类似于JAVA中的ArrayList)
   *  当聚合数据较多时,会消耗大量的网络传输资源, 以及很大的内存存储聚合数据, 效率较低
   */
  def cogroupNonShuffle(inputRDD1: RDD[(Int, Char)], inputRDD2: RDD[(Int, Char)]): Unit = {
    val resultRDD = inputRDD1.partitionBy(new HashPartitioner(3))
      .cogroup(inputRDD2)
  }

  /**
   * join类似于sql里面的join() leftOuterJoin,rightOuterJoin,fullOuterJoin
   * join操作实际上建立在cogroup上, join先调用cogroup生成CoGroupedRDD和MapPartitionsRDD
   * 然后计算MapPartitionsRDD中的笛卡尔积, 生成新的MapPartitionsRDD
   * 是否shuffle受RDD依赖关系和, partitioner类型和分区个数影响
   */
  def join(inputRDD1: RDD[(Int, Char)], inputRDD2: RDD[(Int, Char)]): Unit = {
    // inputRDD1.partitionBy(new HashPartitioner(3))
    // inputRDD2.partitionBy(new HashPartitioner(3))

    val resultRDD = inputRDD1.join(inputRDD2, 3)
  }

  /**
   * 计算两个RDD的笛卡尔积
   * 如果RDD1有m个分区, RDD2有n个分区, 那么cartesian会操作m*n个分区
   * 虽然依赖关系复杂, 但是属于窄依赖, 不属于shuffle依赖
   */
  def cartesian(inputRDD1: RDD[(Int, Char)], inputRDD2: RDD[(Int, Char)]): Unit = {
    val resultRDD = inputRDD1.cartesian(inputRDD2)
  }

  /**
   * 求交集时将RDD1和RDD2中共同的元素抽取出来, 形成新的RDD3
   * 先使用cogroup将相同record聚合在一起, 过滤出两个RDD都存在的record
   * record转化为<K,V(null)>, 聚合过后过滤掉"()"的record
   */
  def intersection(sc: SparkSession): Unit = {
    val inputRDD1: RDD[Int] = sc.sparkContext.parallelize(List(2, 2, 3, 4, 5, 6, 8, 6), 3)
    val inputRDD2 = sc.sparkContext.parallelize(List(2, 3, 6, 6), 2)
    inputRDD1.intersection(inputRDD2, 2)
  }

  /**
   * 数据去重,
   * 和intersection类似 record转化为<K,V(null)> -> reduceByKey聚合数据 -> map取key
   */
  def distinct(sc: SparkSession): Unit = {
    val inputRDD = sc.sparkContext.parallelize(List(1, 2, 2, 3, 2, 1, 4, 3, 4), 3)
    val resultRDD = inputRDD.distinct()
  }

  /**
   * 合并元素, 得到新的RDD
   * RangeDependency, 如果childRDD类型不同, union结果会有区别
   */
  def union(sc: SparkSession): Unit = {
    val inputRDD1 = sc.sparkContext.parallelize(List(2, 2, 3, 4, 5, 6, 8, 6), 3)
    val inputRDD2 = sc.sparkContext.parallelize(List(2, 3, 6, 6), 2)
    // childRDD分区个数不同 -> 新的RDD为childRDD分区个数之和 -> 新的RDD每个分区和childRDD一一对应
    inputRDD1.union(inputRDD2)

    val inputRDD4 = sc.sparkContext.parallelize(Array[(Int, Char)](
      (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e'), (3, 'f'), (2, 'g'), (1, 'h'), (2, 'i')
    ), 2)
    val inputRDD3 = sc.sparkContext.parallelize(Array[(Int, Char)](
      (1, 'A'), (2, 'B'), (3, 'C'), (4, 'D'),  (6, 'E')
    ), 2)
    // childRDD分区个数相同 -> 新的RDD分区个数和childRDD一致 -> 新的RDD分区数据对应childRDD的每一个分区
    inputRDD3.union(inputRDD4)
  }

}
