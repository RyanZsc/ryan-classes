package com.ryan.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark06_RDD_Operator_Transform_Test {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    // TODO 算子 - groupBy
    val rdd = sc.textFile("datas/apache.log")

    val timeRDD: RDD[(String, Iterable[(String, Int)])] = rdd.map(line => (line.split(" ")(3).split(":")(1),1)).groupBy(_._1)

    timeRDD.map(line => (line._1,line._2.size)).collect().foreach(println)

    sc.stop()
  }
}
