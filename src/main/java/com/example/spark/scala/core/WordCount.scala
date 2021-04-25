package com.example.spark.scala.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import java.io.PrintWriter

/**
 * @author xyh
 * @date 2021/4/2 23:02
 */
object WordCount {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("local_test")
    val sc = new SparkContext(sparkConf)

    val lines: RDD[String] = sc.textFile("datas")
    val words: RDD[String] = lines.flatMap(_.split(" "))
    val wordGroup: RDD[(String, Iterable[String])] = words.groupBy(word => word)

    val wordToCount = wordGroup.map {
      case( word,list) => {
        (word,list.size)
      }
    }
    val array: Array[(String, Int)] = wordToCount.collect()
    array.foreach(println)


    //改良版
    val wordToOne = words.map(
      word => (word, 1)
    )
    val wordGroup2 = wordToOne.groupBy(
      t => t._1
    )
    val wordToCount2 = wordGroup2.map {
      case (word, list) => {
        list.reduce(
          (t1, t2) => {
            (t1._1, t1._2 + t2._2)
          }
        )
      }
    }

    val array2: Array[(String, Int)] = wordToCount2.collect()
    array2.foreach(println)


    //spark版
    val wordToCount3 = wordToOne.reduceByKey(_ + _)
    val array3: Array[(String, Int)] = wordToCount2.collect()
    array3.foreach(println)



    sc.stop()
  }
}
