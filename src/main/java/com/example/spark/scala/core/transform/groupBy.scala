package com.example.spark.scala.core.transform

import java.text.SimpleDateFormat

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author xyh
 * @date 2021/4/30 16:15
 */
object groupBy {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("local_test")
    val sc = new SparkContext(sparkConf)

    //根据单词开头的字符进行分组,这里需要注意，分组和分区没有必然关系,一个分区中可能有多个组，或者只有一个组
    val RDD: RDD[String] = sc.makeRDD(List("spark","hadoop", "scala","hello"),2)
    val groupRDD = RDD.groupBy(_.charAt(0))

    groupRDD.collect().foreach(println(_))

    //通过分组统计不同时间段日志的数量
    val logRDD = sc.textFile("datas/apache.log")
    val mappleRDD = logRDD.map(
      line => {
        val time = line.split(" ")(3)
        val sdf = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
        val date = sdf.parse(time)
        val hours = date.getHours
        (hours, 1)
      }
    )
//            .reduceByKey(_+_)
      .groupBy(_._1)

    val resultRDD = mappleRDD.map {
      case (hour, iter) => {
        (hour, iter.size)
      }
    }

    resultRDD.collect().foreach(println(_))

    sc.stop()
  }
}
