package cn.locusc.bd.spark.scala.daios.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{HashPartitioner, RangePartitioner}

/**
 * @author Jay
 * 分区算子示例
 * 2022/12/13
 */
object PartitionOperator {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("operator about partition")
      .master("local")
      .getOrCreate()

    val inputRDD = spark.sparkContext.parallelize(Array[(Int, Char)](
      (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (2, 'e'), (3, 'f'), (2, 'g'), (1, 'h')
    ), 3)

    val inputRD1 = spark.sparkContext.parallelize(Array[(Int, Char)](
      (3, 'c'), (3, 'f'), (1, 'a'), (4, 'd'), (1, 'h'), (2, 'b'), (5, 'e'), (2, 'g')
    ), 5)

    val inputRD2 = spark.sparkContext.parallelize(Array[(Char, Int)](
      ('D', 2), ('B', '4'), ('C', 3), ('A', 5), ('B', 2), ('C', 1), ('C', 3), ('A', 4)
    ), 5)

    partitionBy(inputRDD)
    coalesce(inputRD1)
  }

  /**
   * 根据策略和分区数量将数据分区
   */
  def partitionBy(inputRDD: RDD[(Int, Char)]): Unit = {
    // 使用HashPartitioner重新分区
    inputRDD.partitionBy(new HashPartitioner(2))
    // 使用RangePartitioner重新分区
    inputRDD.partitionBy(new RangePartitioner(2, inputRDD))
  }

  /**
   * 将RDD的分区个数根据指定的值升高或降低
   */
  def coalesce(inputRDD: RDD[(Int, Char)]): Unit = {
    // 将相邻的分区直接合并在一起, 但是在RDD不同分区分区数据差距较大时会造成数据倾斜
    // coalesceRDD中某些分区数据过多或者过少
    val coalesceRDD1 = inputRDD.coalesce(2)
    // 分区个数还是5, 不会增加, coalesce是窄依赖不会将一个分区拆分成多个
    val coalesceRDD2 = inputRDD.coalesce(6)
    // 使用shuffle减少分区个数, 对数据进行混洗, 解决数据倾斜问题, 生成一个特殊key, Spark根据这个key的hash值重新分区
    val coalesceRDD3 = inputRDD.coalesce(2, shuffle = true)
    // 使用shuffle增加分区个数, 通过宽依赖对分区进行拆分和重新组合, 解决分区不能增加的问题
    val coalesceRDD4 = inputRDD.coalesce(6, shuffle = true)
  }

  /**
   * 将RDD中的数据进行重新分区, 和inputRDD.coalesce(num, shuffle = true)一致
   */
  def repartition(inputRDD: RDD[(Int, Char)]): Unit = {
    inputRDD.repartition(6)
  }

  /**
   * 比较repartition可以使用partitioner, 并且会对每个分区中的key进行排序
   * 待定(如果不是RangePartitioner, 不能保证全局有序)
   */
  def repartitionAndSortWithinPartitions(inputRDD: RDD[(Char, Int)]): Unit = {
    inputRDD.repartitionAndSortWithinPartitions(new HashPartitioner(2))
  }

}
