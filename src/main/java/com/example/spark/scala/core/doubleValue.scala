package com.example.spark.scala.core

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author xyh
 * @date 2021/5/10 16:06
 */
object doubleValue {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("doubleValue")
    val sc = new SparkContext(sparkConf)

    val RDD1 = sc.makeRDD(List(1, 2, 3, 4))
    val RDD2 = sc.makeRDD(List(3, 4, 5, 6))

    //除了拉链操作，其他的double value操作要求两个rdd的数据类型保持一致，否则语法会报错

    //交集 (3,4)
    val RDD3 = RDD1.intersection(RDD2)
    println(RDD3.collect().mkString(","))

    //并集 (1,2,3,4,3,4,5,6)
    val RDD4 = RDD1.union(RDD2)
    println(RDD4.collect().mkString(","))

    //差集 (1,2)
    val RDD5 = RDD1.subtract(RDD2)
    println(RDD5.collect().mkString(","))

    //拉链 (1-3,2-4,3-5,4-6)
    //两个rdd要求分区数量保持一致，每个分区的元素数量一致，否则运行报错
    val RDD6 = RDD1.zip(RDD2)
    println(RDD6.collect().mkString(","))

    sc.stop()

  }
}
