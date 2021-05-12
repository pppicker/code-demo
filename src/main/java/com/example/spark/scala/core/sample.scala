package com.example.spark.scala.core

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author xyh
 * @date 2021/5/7 15:58
 */
object sample {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sample")
    val sc = new SparkContext(sparkConf)

    val RDD = sc.makeRDD(List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))
    //抽样，第一个参数表示抽取数据后是否将数据放回 true(放回) false(不放回)
    //第二个参数表示数据源中每条数据被抽中的概率
    //第三个参数表示抽取数据时随机算法的种子
    val sampleRDD = RDD.sample(false, 0.4, 1)
    println(sampleRDD.collect().mkString(","))
  }

}
