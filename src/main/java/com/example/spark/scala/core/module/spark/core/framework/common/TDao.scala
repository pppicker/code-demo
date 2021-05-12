package com.example.spark.scala.core.module.spark.core.framework.common

import com.atguigu.bigdata.spark.core.framework.util.EnvUtil

trait TDao {

    def readFile(path:String) = {
        EnvUtil.take().textFile(path)
    }
}
