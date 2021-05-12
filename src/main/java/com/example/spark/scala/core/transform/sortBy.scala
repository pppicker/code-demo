package com.example.spark.scala.core.transform

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author xyh
 * @date 2021/5/10 15:37
 */
object sortBy {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sortBy")
    val sc = new SparkContext(sparkConf)

//    val RDD = sc.makeRDD(List(1, 3, 7, 2, 9, 8),2)
//
//    val sortRDD = RDD.sortBy(num=>num)
//    sortRDD.saveAsTextFile("output")

    //排序默认为升序，传入false为降序
    //sort默认不会改变分区，但是会进行shuffle操作
    val tupleRDD = sc.makeRDD(List(("1",1),("11",1),("2",1)))
    val sortRDD2 = tupleRDD.sortBy(_._1.toInt)
//    val sortRDD2 = tupleRDD.sortBy(_._1.toInt,false)
    sortRDD2.saveAsTextFile("output/sortBy2")

    sc.stop()

  }

}
