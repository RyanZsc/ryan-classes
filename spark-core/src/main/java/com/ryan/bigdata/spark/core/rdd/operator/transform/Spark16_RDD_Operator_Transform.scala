package com.ryan.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 用于数据倾斜的时候用，分区不同，数据量不同，sample就可以判断
  */
object Spark16_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    // TODO 算子 - groupByKey类型
    // groupByKey ： 将数据源中的数据，相同key的数据分在一个组中，行程一个对偶元组
    //                元组中的第一个元素就是key
    //                元组中的第二个元素就是相同key的value集合
    val rdd = sc.makeRDD(List(
      ("a",1),("a",2),("a",3),("b",4)
    ))

    val groupRDD: RDD[(String, Iterable[Int])] = rdd.groupByKey()
    groupRDD.collect().foreach(println)

    val groupRDD1: RDD[(String, Iterable[(String, Int)])] = rdd.groupBy(_._1)
    groupRDD1.collect().foreach(println)

    /*
       groupByKey会导致数据打乱重组，存在shuffle操作
       spark中，shuffle操作必须落盘处理，不能在内存中数据等在，会导致内存溢出
       因为和磁盘交互，shuffle操作的性能非常低
     */

    /*
      reduceByKey 支持分区内预聚合功能，可以有效减少shuffle时落盘的数量，提升shuffle的性能
      reduceByKey 要求分区内和分区间计算规则是相同的。
     */

    sc.stop()
  }
}
