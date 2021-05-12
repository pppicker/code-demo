package com.example.spark.scala.core

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author xyh
 * @date 2021/5/10 17:04
 */
object groupByKey {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("groupByKey")
    val sc = new SparkContext(sparkConf)

    val RDD = sc.makeRDD(List(("a",1),("a",2),("a",3),("b",4)))

    //groupByKey与groupBy区别：
    //groupBy可以自定义分组对象，groupByKey只能通过key来分组
    //groupBy返回 （指定分区对象,ite[(key,value)]） ， groupByKey返回 （key,ite[value])
    val groupRDD = RDD.groupByKey()
    println(groupRDD.collect().mkString(","))

    sc.stop()
  }

}
