package com.ryan.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 用于数据倾斜的时候用，分区不同，数据量不同，sample就可以判断
  */
object Spark13_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    // TODO 算子 - 双value类型
    // 交集、并集、差集、要求两个数据源数据类型保持一致
    // 拉链操作两个数据源类型可以不一致
    val rdd1 = sc.makeRDD(List(1,2,3,4))
    val rdd2 = sc.makeRDD(List(3,4,5,6))
    val rdd3 = sc.makeRDD(List("3","4","5","6"))

    // 交集
    println(rdd1.intersection(rdd2).collect().mkString(","))
//    rdd1.intersection(rdd3)

    // 并集
    println(rdd1.union(rdd2).collect().mkString(","))

    // 差集
    println(rdd1.subtract(rdd2).collect().mkString(","))

    // 拉链
    println(rdd1.zip(rdd2).collect().mkString(","))
    println(rdd1.zip(rdd3).collect().mkString(","))

    sc.stop()
  }
}
