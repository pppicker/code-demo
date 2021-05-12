package com.example.spark.scala.core

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author xyh
 * @date 2021/5/7 16:56
 */
object coalesce {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("coalesce")
    val sc = new SparkContext(sparkConf)

    val RDD = sc.makeRDD(List(1, 2, 3, 4),4)

    //缩减分区，用于大数据集过滤后,提高小数据集的执行效率
    //coalesce是将多余的分区直接合并，可能会导致数据不均衡,如果想让数据均衡，可以传入第二个参数：true,进行shuffle处理
    //coalesce也可以扩大分区，这时必须传入true参数，即进行shuffle操作,因为扩大分区必须进行shuffle
    //一般扩大分区使用repartition,他的底层就是调用coalesce传入true参数
    val coaRDD = RDD.coalesce(2)
    coaRDD.saveAsTextFile("output")
    sc.stop()

  }
}
