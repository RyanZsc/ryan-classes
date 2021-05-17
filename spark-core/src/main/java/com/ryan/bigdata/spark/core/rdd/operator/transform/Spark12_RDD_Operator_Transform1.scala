package com.ryan.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 用于数据倾斜的时候用，分区不同，数据量不同，sample就可以判断
  */
object Spark12_RDD_Operator_Transform1 {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    // TODO 算子 - sortBy
    val rdd = sc.makeRDD(List(("1",1),("11",2),("2",3)),2)

    // sortBy 可以根据制定的规则对数据源中的数据进行排序，默认为升序，第二个参数可以改变排序方式
    // sortBy 默认情况下不会改变分区，但是中间存在shuffle操作
    rdd.sortBy(t=>t._1.toInt,ascending = false).collect().foreach(println)

    sc.stop()
  }
}
