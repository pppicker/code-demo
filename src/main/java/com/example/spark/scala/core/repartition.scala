package com.example.spark.scala.core

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author xyh
 * @date 2021/5/10 15:27
 */
object repartition {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("coalesce")
    val sc = new SparkContext(sparkConf)

    val RDD = sc.makeRDD(List(1, 2, 3, 4, 5, 6),2)
    val repartitionRDD = RDD.repartition(3)
    repartitionRDD.saveAsTextFile("output/repartition")


    sc.stop()
  }

}
