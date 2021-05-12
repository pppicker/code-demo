package com.example.spark.scala.core.transform

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author xyh
 * @date 2021/4/22 11:35
 */
object test {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("local_test")
    //手动指定初始化RDD分区，如果不指定，本地模式默认读取cpu核数
    sparkConf.set("spark.default.parallelism", "4")
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("WARN")


    //创建RDD,从内存创建
    val seq = Seq[Int](1,2,3,4,5)
    val memRDD1 = sc.parallelize(seq,4)
    val memRDD2 = sc.makeRDD(seq,4)

    memRDD1.saveAsTextFile("output")

    //创建RDD,从文件创建,不传入分区参数默认使用cpu核数的值，这个值会被文件内容的字节数进行整除，余数的内容放入多余的分区里
    val fileRDD1 = sc.textFile("datas/1.txt",2)
    val fileRDD2 = sc.textFile("datas")

    fileRDD1.saveAsTextFile("output")

//    fileRDD2.collect().foreach(println(_))

    sc.stop()
  }

}
