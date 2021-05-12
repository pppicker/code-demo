package com.example.spark.scala.core.transform

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

    //如果不需要初始值，可以使用combineByKey

    // TODO 算子 - (Key - Value类型)

    val rdd = sc.makeRDD(List(
      ("a", 1), ("a", 2), ("b", 3),
      ("b", 4), ("b", 5), ("a", 6)
    ),2)

    /*
    reduceByKey:

         combineByKeyWithClassTag[V](
             (v: V) => v, // 第一个值不会参与计算
             func, // 分区内计算规则
             func, // 分区间计算规则
             )

    aggregateByKey :

        combineByKeyWithClassTag[U](
            (v: V) => cleanedSeqOp(createZero(), v), // 初始值和第一个key的value值进行的分区内数据操作
            cleanedSeqOp, // 分区内计算规则
            combOp,       // 分区间计算规则
            )

    foldByKey:

        combineByKeyWithClassTag[V](
            (v: V) => cleanedFunc(createZero(), v), // 初始值和第一个key的value值进行的分区内数据操作
            cleanedFunc,  // 分区内计算规则
            cleanedFunc,  // 分区间计算规则
            )

    combineByKey :

        combineByKeyWithClassTag(
            createCombiner,  // 相同key的第一条数据进行的处理函数
            mergeValue,      // 表示分区内数据的处理函数
            mergeCombiners,  // 表示分区间数据的处理函数
            )

     */

    rdd.reduceByKey(_+_) // wordcount
    rdd.aggregateByKey(0)(_+_, _+_) // wordcount
    rdd.foldByKey(0)(_+_) // wordcount
    rdd.combineByKey(v=>v,(x:Int,y)=>x+y,(x:Int,y:Int)=>x+y) // wordcount

    sc.stop()



    sc.stop()
  }

}
