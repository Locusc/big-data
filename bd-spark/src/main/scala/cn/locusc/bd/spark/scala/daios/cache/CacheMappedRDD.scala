package cn.locusc.bd.spark.scala.daios.cache

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

object CacheMappedRDD {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("CacheMappedRDD")
      .master("local[1]")
      .getOrCreate()

    val inputRDD = spark.sparkContext.parallelize(Array[(Int, String)](
      (1, "a"), (2, "b"), (3, "c"), (4, "d"), (5, "e"), (3, "f"), (2, "g"), (1, "h"), (2, "i")
    ), 3)

    cacheMappedRDD(inputRDD)
  }

  /**
   * 共享mappedRDD的应用程序
   * PAGE 181
   */
  def cacheMappedRDD(inputRDD: RDD[(Int, String)]): Unit = {
    val mappedRDD = inputRDD.map(m => (m._1 + 1, m._2))
    mappedRDD.cache()
    val reducedRDD = mappedRDD.reduceByKey((x, y) => x + "_" + y, 2)
    reducedRDD.foreach(println)
    val groupedRDD = mappedRDD.groupByKey(3).mapValues(V => V.toList)
    groupedRDD.foreach(println)
  }

  /**
   * 复杂数据缓存示例
   * PAGE 184
   */
  def complexCacheRDD(inputRDD: RDD[(Int, String)]): Unit = {
    val mappedRDD = inputRDD.map(m => (m._1 + 1, m._2))
    // 添加缓存语句
    mappedRDD.cache()
    val reducedRDD = mappedRDD.reduceByKey((x, y) => x + "_" + y, 2)
    // 添加缓存语句
    reducedRDD.cache()
    reducedRDD.foreach(println)
    val groupedRDD = mappedRDD.groupByKey().mapValues(V => V.toList)
    // 添加缓存语句
    groupedRDD.cache()
    groupedRDD.foreach(println)
    // 添加join()语句
    val joinedRDD = reducedRDD.join(groupedRDD)
    // 添加输出语句
    joinedRDD.foreach(println)
  }

  /**
   * 使用persist算子可以手动回收缓存
   * 在错误的位置回收缓存会导致从头开始从新计算
   */
  def persistRDD(inputRDD: RDD[(Int, String)]): Unit = {
    val mappedRDD = inputRDD.map(m => (m._1 + 1, m._2))
    // or mappedRDD.cache()
    mappedRDD.persist(StorageLevel.MEMORY_ONLY)

    val reducedRDD = mappedRDD.reduceByKey((x, y) => x + "_" + y, 2)
    // 同步回收内存
    mappedRDD.unpersist(blocking = true)
    // 错误回收位置
    reducedRDD.foreach(println)

    val groupedRDD = mappedRDD.groupByKey().mapValues(V => V.toList)
    // 错误回收位置
    mappedRDD.unpersist(blocking = true)

    groupedRDD.foreach(println)
    // 异步回收缓存 正确回收位置
    mappedRDD.unpersist(blocking = false)
  }

}
