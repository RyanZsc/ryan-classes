package com.ryan.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object Spark04_RDD_Operator_Transform1 {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    // TODO 算子 - mapPartitions
    val rdd = sc.makeRDD(List(
      "Hello Scala", "Hello Spark"
    ))

    val flatRDD = rdd.flatMap(_.split(" "))

    flatRDD.collect().foreach(println)

    sc.stop()
  }
}
