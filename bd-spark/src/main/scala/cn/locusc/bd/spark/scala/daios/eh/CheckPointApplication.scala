package cn.locusc.bd.spark.scala.daios.eh

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
 * @author Jay
 * checkpoint机制示例
 * 2023/2/8
 */
object CheckPointApplication {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("CheckPointApplication")
      .master("local[1]")
      .getOrCreate()

    val sc = spark.sparkContext

    // 首先设置持久化路径 一般是HDFS
    sc.setCheckpointDir("hdfs://checkpoint")

    val data = sc.parallelize(Array[(Int, String)](
      (1, "a"), (2, "b"), (3, "c"), (4, "d"), (5, "e"), (3, "f"), (2, "g"), (1, "h")
    ), 3)

    simpleCheckPoint(data)
  }

  /**
   * 简单的checkpoint
   * 为了避免checkpoint增加job执行耗时
   * spark将需要checkpoint的RDD会开启一个额外的job进行计算和持久化
   */
  def simpleCheckPoint(data: RDD[(Int, String)]): Unit = {
    // 对输入数据进行一些计算
    val pairs = data.map(m => (m._1 + 10, m._2))
    // 对中间数据pairs: MapPartitionsRDD进行checkpoint
    pairs.checkpoint()
    // 生成第一个job
    pairs.count()
    // 在checkpoint的数据上面再次进行计算
    val result = pairs.groupByKey(2)
    result.checkpoint()
    // 生成第二个job
    result.foreach(println)
  }

  /**
   * 书中的spark版本, 这个地方两个job都没有被checkpoint
   * result没有被checkpoint是因为job结束之后不会对需要checkpoint的数据进行标识
   * pairs没有被checkpoint作者怀疑有BUG
   */
  def afterCheckPoint(data: RDD[(Int, String)]): Unit = {
    val pairs = data.map(m => (m._1 + 10, m._2))
    // 第一个job
    pairs.count()
    // 对中间数据pairs: MapPartitionsRDD进行checkpoint
    pairs.checkpoint()
    // 在checkpoint的数据上面再次进行计算
    val result = pairs.groupByKey(2)
    // 第二个job
    result.foreach(println)
    result.checkpoint()
  }

  /**
   * 这里只有result被checkpoint
   * spark的checkpoint机制是从后往前扫描, 这里先扫描到result
   * 并且会切断result往上的引用链lineage, 告诉后面算子从这里开始读取数据, 同时导致pairs不会被checkpoint
   *
   * (待确认, 不仅是因为lineage关系, 也有可能是result上面已经不存在action操作了)
   */
  def multipleCheckPoint(data: RDD[(Int, String)]): Unit = {
    val pairs = data.map(m => (m._1 + 10, m._2))
    pairs.checkpoint()
    val result = pairs.groupByKey(2)
    result.checkpoint()
    // 只有一个job
    result.foreach(println)
  }

  /**
   * 生成三个job
   * 1.先对pairs进行缓存
   * 2.读取缓存中的pairs进行checkpoint
   * 3.最后读取缓存中的数据进行计算
   * 这里pairs以及被缓存了就不会使用checkpoint的数据, 缓存优先级更高, 因为checkpoint需要操作磁盘会带来IO, 没有内存缓存速度快
   */
  def cacheCheckPoint(data: RDD[(Int, String)]): Unit = {
    val pairs = data.map(m => (m._1 + 10, m._2))
    pairs.cache()
    pairs.checkpoint()
    pairs.count()
    val result = pairs.groupByKey(3)
    result.foreach(println)
  }

  /**
   * 对两个RDD既进行缓存又进行checkpoint
   * 生成5个job
   * 1.对mappedRDD以及reducedRDD进行缓存
   * 2.对reducedRDD进行checkpoint
   * 3.读取缓存的mappedRDD正常计算得到groupedRDD
   * 4.对groupedRDD进行checkpoint
   * 5.读取缓存的reducedRDD以及groupedRDD进行join操作
   */
  def multipleRDDCacheCheckPoint(data: RDD[(Int, String)]): Unit = {
    val mappedRDD = data.map(m => (m._1 + 10, m._2))
    mappedRDD.cache()
    val reducedRDD = mappedRDD.reduceByKey((x, y) => x + "_" + y, 2)
    reducedRDD.cache()
    reducedRDD.checkpoint()
    reducedRDD.foreach(println)
    val groupedRDD = mappedRDD.groupByKey().mapValues(V => V.toList)
    groupedRDD.cache()
    groupedRDD.checkpoint()
    groupedRDD.foreach(println)
    val joinedRDD = reducedRDD.join(groupedRDD)
    joinedRDD.foreach(println)
  }

}
