package cn.locusc.bd.spark.scala.daios.operator.action

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
 * @author Jay
 * reduce算子示例
 * 2022/12/24
 */
object ReduceOperator {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("operator about reduce")
      .master("local[2]")
      .getOrCreate()

    val inputRDD1 = spark.sparkContext.parallelize(Array[String](
      "a", "b", "c", "d", "e", "f", "g", "h", "i"
    ), 3)

    val inputRDD2 = spark.sparkContext.parallelize(1 to 18, 6).map(m => m + "")
    treeAggregate(inputRDD2)

    val inputRDD3 = spark.sparkContext.parallelize(Array[(Int, String)](
      (1, "a"), (2, "b"), (3, "c"), (4, "d"), (2, "e"), (3, "f"), (2, "g"), (1, "h"), (2, "i")
    ), 3)
    reduceByKeyLocality(inputRDD3)
  }

  /**
   * 将RDD中的record按照func进行聚合, func语义与foldByKey(func)中的func相同,
   * 区别在于不会生成新的RDD, fold直接计算出结果
   */
  def fold(inputRDD: RDD[String]): Unit = {
    val result = inputRDD.fold("0")((x, y) => x + "_" + y)
    println(result)
  }

  /**
   * 将RDD中record按照func进行聚合, func语义与reduceByKey(func)中的func相同
   * reduce(func)的语义和去掉初始值的fold(func相同), 并且可以看作是aggregate(seqOp,comOp)中seqOp=comOp=func
   */
  def reduce(inputRDD: RDD[String]): Unit = {
    val result = inputRDD.reduce((x, y) => x + "_" + y)
    println(result)
  }

  /**
   * 将RDD中record进行聚合, seqOp和comOp的语义与aggregateByKey(zeroValue)(seqOp, comOp)中类似
   * seqOp,comOp聚合时zeroValue都会参与计算, 而在aggregateByKey中, 初始值只参与seqOp计算
   */
  def aggregate(inputRDD: RDD[String]): Unit = {
    val result = inputRDD.aggregate("0")((x, y) => x + "_" + y, (x, y) => x + "=" + y)
    println(result)
  }

  /**
   * 上述三个算子的共同问题是, 当需要merge的部分结果很大时, 数据传输量很大, 而且Driver是单点merge
   * 存在效率和内存空间限制问题, 为了解决这个问题, spark对这些聚合操作进行了优化, 提出了treeAggregate和treeReduce
   *
   * 将RDD中record按照树形结构进行聚合, seqOp和comOp的语义与aggregate中的相同, 树的高度(depth)的默认值为2
   * spark采用foldByKey来实现非根节点的聚合(类似归并排序中的层次归并), 并使用fold来实现根节点的聚合
   * 如果分区过少, 会退化为aggregate
   *
   * treeAggregate为每个record添加一个特殊的key, 使得RDD中的数据被均分到每个非根节点进行聚合
   */
  def treeAggregate(inputRDD: RDD[String]): Unit = {
    inputRDD.treeAggregate("0")((x, y) => x + "_" + y, (x, y) => x + "=" + y, 2)
  }

  /**
   * treeReduce实际上是调用treeAggregate实现的, 唯一区别是没有初始值zeroValue
   */
  def treeReduce(inputRDD: RDD[String]): Unit = {
    inputRDD.treeReduce((x, y) => x + "_" + y, 2)
  }

  /**
   * 将RDD中的record按照Key进行reduce(), 不同于reduceByKey,
   * reduceByKeyLocality首先在本地进行局部reduce(), 然后把数据汇总到Driver端进行全局reduce,
   * 返回的结果存放在HashMap中而不是RDD中
   */
  def reduceByKeyLocality(inputRDD: RDD[(Int, String)]): Unit = {
    inputRDD.reduceByKeyLocally((x, y) => x + "_" + y)
  }

}
