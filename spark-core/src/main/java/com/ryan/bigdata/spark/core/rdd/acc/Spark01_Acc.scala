package com.ryan.bigdata.spark.core.rdd.acc

import org.apache.spark.{SparkConf, SparkContext}

object Spark01_Acc {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)

    val rdd = sc.makeRDD(List(1,2,3,4))

    // reduce: 分区内计算，分区间计算
//    val i = rdd.reduce(_+_)
//    println(i)

    var sum = 0
    rdd.foreach(
      num => {
//        println(sum)
        sum += num
      }
    )
    println("sum = " + sum)

    sc.stop()
  }
}
