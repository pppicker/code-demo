package com.example.spark.scala.core

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author xyh
 * @date 2021/5/7 15:22
 */
object filter {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("filter")
    val sc = new SparkContext(sparkConf)

    val logRDD = sc.textFile("datas/apache.log")
    val filterRDD = logRDD.filter(
      line => {
        line.split(" ")(3).startsWith("07/05/2021")
      }
    )

    filterRDD.collect().foreach(println(_))

    sc.stop()

  }

}
