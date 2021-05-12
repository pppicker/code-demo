package com.example.spark.scala.core

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @author xyh
 * @date 2021/4/30 15:13
 */
object mapPartitionsWithIndex {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("local_test")
    val sc = new SparkContext(sparkConf)

    val RDD = sc.makeRDD(List(1, 2, 3, 4, 5), 2)
    val resultRDD = RDD.mapPartitionsWithIndex(
      (index, iter) => {
        if (index == 1) {
          iter
        } else {
          Nil.iterator
        }
      }
    )
    resultRDD.collect().foreach(println(_))
    sc.stop()
  }

}
