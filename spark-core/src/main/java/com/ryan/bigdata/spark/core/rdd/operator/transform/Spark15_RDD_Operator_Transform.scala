package com.ryan.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**
  * 用于数据倾斜的时候用，分区不同，数据量不同，sample就可以判断
  */
object Spark15_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    // TODO 算子 - reduceByKey类型
    // 相同的key的数据进行value数据的聚合操作
    // scala 语言中一般的聚合都是两两聚合, spark是基于scala开发，所以他的聚合也是两两聚合
    // reduceByKey 中如果key的数据只有一个，是不会参与运算
    sc.makeRDD(List(
      ("a",1),("a",2),("a",3),("b",4)
    )).reduceByKey((x: Int, y: Int) =>{
      println(s"x=$x, y=$y")
      x + y
    }).collect().foreach(println)

    sc.stop()
  }
}
