package com.ryan.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 用于数据倾斜的时候用，分区不同，数据量不同，sample就可以判断
  */
object Spark11_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    // TODO 算子 - coalesce
    val rdd = sc.makeRDD(List(1,2,3,4,5,6),2)

    // coalesce算子可以扩大分区，但是如果不进行shuffle操作，则没有任何意义，不起作用
    // 如果想实现扩大分区，则需要shuffle
    // spark提供了个简化操作
    // 缩减分区: coalesce，如果想要数据均衡，则就shuffle
    // 扩大分区：repartition，底层就是调用coalesce，则用的书shuffle
//    val newRDD: RDD[Int] = rdd.coalesce(3,shuffle = true)
    val newRDD: RDD[Int] = rdd.repartition(3)

    newRDD.saveAsTextFile("output")

    sc.stop()
  }
}
