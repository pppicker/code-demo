package com.example.spark.scala.core.transform

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**
 * @author xyh
 * @date 2021/5/10 16:30
 */
object keyValue {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("keyValue")
    val sc = new SparkContext(sparkConf)

    val RDD = sc.makeRDD(List(1, 2, 3, 4),2)
    val kvRDD = RDD.map((_,1))
    //隐式转换 （二次编译）
    //partitionBy 根据指定的分区规则，对数据进行重分区 与coalesce和repartition区别：这两个是改变分区数量，partitionBy是改变数据分区规则
    val partitionRDD = kvRDD.partitionBy(new HashPartitioner(2))
    partitionRDD.saveAsTextFile("output/partitionBy")

    sc.stop()


  }

}
