package cn.locusc.bd.spark.scala.daios.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
 * @author Jay
 * zip算子示例
 * 2022/12/17
 */
object ZipOperation {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("operator about zip")
      .master("local")
      .getOrCreate()

    val inputRDD1 = spark.sparkContext.parallelize(Array[(Int, Char)](
      (1, 'a'), (1, 'b'), (2, 'c'), (3, 'd'), (4, 'a'), (5, 'f')
    ), 3)

    val inputRDD2 = spark.sparkContext.parallelize(Array[(Int, Char)](
      (1, 'f'), (3, 'g'), (2, 'h'), (4, 'i')
    ), 3)

    zipPartitions(inputRDD1, inputRDD2)
  }

  /**
   * 将RDD1和RDD2中的元素按照一一对应的关系(拉链)连接在一起
   * 构成<K,V> record, K来自于RDD1, V来自于RDD2,
   * 要求分区个数相同, 分区中的元素个数相同, 生成ZippedPartitionsRDD2
   */
  def zip(ss: SparkSession): Unit = {
    val inputRDD1 = ss.sparkContext.parallelize(1 to 8, 3)
    val inputRDD2 = ss.sparkContext.parallelize('a' to 'h', 3)
    inputRDD1.zip(inputRDD2);
  }

  /**
   * 和zip相似, 要求分区个数相同但是元素可以不相同, 可以自定义函数对zip后的RDD进行处理,
   * 并且preservesPartitioning可以选择是否延用parentRDD的partitioner, 不选择spark会认为RDD3是随机划分的
   * 主要为了避免shuffle
   */
  def zipPartitions(inputRDD1: RDD[(Int, Char)], inputRDD2: RDD[(Int, Char)]): Unit = {
    inputRDD1.zipPartitions(inputRDD2, preservesPartitioning = false)({
      (rdd1Iter, rdd2Iter) => {
        var result = List[String]()
        while (rdd1Iter.hasNext && rdd2Iter.hasNext) {
          // 将RDD1和RDD2中的数据按照下划线连接, 然后添加到result:List的首位
          result ::= rdd1Iter.next() + "_" + rdd2Iter.next()
        }
        result.iterator
      }
    })
  }

  /**
   * 对RDD1中的数据进行编号, 编号方式是从0开始按序递增(跨分区)
   *
   */
  def zipWithIndex(inputRDD1: RDD[(Int, Char)]): Unit = {
    val resultRDD = inputRDD1.zipWithIndex();
  }

  /**
   * 对RDD1中的数据进行编号, 编号方式为round-robin(类似发牌, 每个人分区轮流编号, 如果没有对应的record, 则轮空并且编号作废)
   */
  def zipWithUniqueId(inputRDD1: RDD[(Int, Char)]): Unit = {
    val resultRDD = inputRDD1.zipWithUniqueId()
  }

}
