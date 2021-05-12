package com.example.spark.scala.core.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author xyh
 * @date 2021/4/30 16:00
 */
object glom {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("local_test")
    val sc = new SparkContext(sparkConf)

    //使用glom将每个分区分散的数据封装成array，然后求所有分区最大值的和
    val RDD: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4),2)
    val glomRDD = RDD.glom()
    val maxRDD = glomRDD.map(
      array => array.max
    )

    println(maxRDD.collect().sum)

  }

}
