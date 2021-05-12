package com.example.spark.scala.core

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author xyh
 * @date 2021/5/10 23:28
 */
object aggregateByKey {

  //TODO 实现逻辑：分区内求最大值，分区间最大值求和
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("aggregateByKey")
    val sc = new SparkContext(sparkConf)

    val RDD = sc.makeRDD(List(("a",1),("a",2),("a",3),("a",4)),2)

    //将数据根据不同规则进行分区内计算和分区间计算
    //aggregateByKey存在函数柯里化,有两个参数列表
    //第一个参数列表
    //  当碰见第一个key时，和value进行分区内计算
    //第二个参数列表需要传入两个参数
    //  第一个参数表示分区内计算规则
    //  第二个参数表示分区间计算规则
    RDD.aggregateByKey(0)(
      (x,y) => math.max(x,y),
      (x,y) => x+y
    )

    //如果分区内和分区间的计算逻辑相同，可以使用foldByKey



    sc.stop()
  }

}
