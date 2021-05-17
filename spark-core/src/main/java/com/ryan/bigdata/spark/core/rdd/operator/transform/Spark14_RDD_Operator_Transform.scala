package com.ryan.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**
  * 用于数据倾斜的时候用，分区不同，数据量不同，sample就可以判断
  */
object Spark14_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    // TODO 算子 -(key value类型)
    val rdd = sc.makeRDD(List(1,2,3,4),2)

    // 隐式转换（二次编译）
    // partitionBy根据制定的分区规则 对数据进行重分区
    rdd.map((_,1)).partitionBy(new HashPartitioner(2)).saveAsTextFile("output")

    sc.stop()
  }
}
