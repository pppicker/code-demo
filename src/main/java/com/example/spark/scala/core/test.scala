package com.example.spark.scala.core

import java.io.PrintWriter

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @author xyh
 * @date 2021/4/22 11:35
 */
object test {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("local_test")
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("WARN")

    val sourceFlat = sc.parallelize(1 to 3)
    sourceFlat.collect()

    val flatMap = sourceFlat.flatMap(1 to _)
    flatMap.collect()
    flatMap.foreach(println(_))
    sc.stop()
  }

}
