package com.example.spark.scala.core.transform

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author xyh
 * @date 2021/4/30 15:27
 */
object flatMap {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("local_test")
    val sc = new SparkContext(sparkConf)

    val RDD = sc.makeRDD(List(List(1, 2), List(3, 4)))
    val resultRDD = RDD.flatMap(list => list)
    resultRDD.collect().foreach(println(_))

    //使用模式匹配将不同类型数据转换为同一种类型
    val RDD2 = sc.makeRDD(List(List(1,2),3,List(4,5)))
    val resultRDD2 = RDD2.flatMap(
      data => {
        data match {
          case list: List[_] => list
          case dat => List(dat)
        }
      }
    )
    resultRDD2.collect().foreach(println(_))






    sc.stop()
  }

}
