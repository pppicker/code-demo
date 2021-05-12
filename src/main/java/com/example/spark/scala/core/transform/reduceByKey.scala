package com.example.spark.scala.core.transform

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author xyh
 * @date 2021/5/10 16:59
 */
object reduceByKey {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("reduceByKey")
    val sc = new SparkContext(sparkConf)

    val RDD = sc.makeRDD(List(("a",1),("a",2),("a",3),("b",4)),2)

    //reduceByKey支持分区预聚合功能，可以减少shuffle时落盘的数据量,提升shuffle性能
    val reduceRDD = RDD.reduceByKey(_ + _)
    println(reduceRDD.collect().mkString(","))

    sc.stop()
  }

}
