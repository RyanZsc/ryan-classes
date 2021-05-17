package com.ryan.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 用于数据倾斜的时候用，分区不同，数据量不同，sample就可以判断
  */
object Spark13_RDD_Operator_Transform1 {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    // TODO 算子 - 双value类型
    // Can't zip RDDs with unequal numbers of partitions: List(2, 4)
    // zip操作 两个数据源分区要保持一致
    // Can only zip RDDs with same number of elements in each partition
    // zip操作 两个数据源要求分区中数据保持一致
    val rdd1 = sc.makeRDD(List(1,2,3,4,5,6),2)
    val rdd2 = sc.makeRDD(List(3,4,5,6),2)

    // 拉链
    println(rdd1.zip(rdd2).collect().mkString(","))

    sc.stop()
  }
}
