package com.ryan.bigdata.spark.core.rdd.acc

import org.apache.spark.{SparkConf, SparkContext}

object Spark03_Acc {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)

    val rdd = sc.makeRDD(List(1,2,3,4))

    // 获取系统的累加器
    //  Spark默认就提供了简单数据聚合的累加器
    val sumAcc = sc.longAccumulator("sum")

//    sc.doubleAccumulator()
//    sc.collectionAccumulator()

    val mapEDD = rdd.map(
      num => {
        // 使用累加器
        sumAcc.add(num)
        num
      }
    )

    // 获取累加器的值
    // 少加：转换算子中调用累加器，如果没有行动算子的话，那么不会执行
    // 多加：转换算子中调用累加器，如果没有行动算子的话，那么不会执行
    // 一般情况下，累加器会放置在行动算子中进行操作
    mapEDD.collect()
    mapEDD.collect()
    println(sumAcc.value)
    
    sc.stop()
  }
}
