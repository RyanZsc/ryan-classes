package com.ryan.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 用于数据倾斜的时候用，分区不同，数据量不同，sample就可以判断
  */
object Spark17_RDD_Operator_Transform2 {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    // TODO 算子 - aggregateByKey
    val rdd = sc.makeRDD(List(
      ("a",1),("a",2),("a",3),("a",4)
    ),2)

    rdd.aggregateByKey(0)(_+_,_+_).collect().foreach(println)

    // 如果聚合计算时，分区内和分区间计算规则相同，spark提供了简化的方法
    rdd.foldByKey(0)(_+_).collect().foreach(println)
    sc.stop()
  }
}
