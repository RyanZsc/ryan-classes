package com.ryan.bigdata.spark.core.rdd.acc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object Spark05_Bc {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)

    val rdd1 = sc.makeRDD(List(
      ("a", 1), ("b", 2), ("c", 3)
    ))
//    val rdd2 = sc.makeRDD(List(
//      ("a", 4), ("b", 5), ("c", 6)
//    ))

    val map = mutable.Map(("a", 4), ("b", 5), ("c", 6))

    // join回导致数据量的几何增长，并且影响shuffle性能，不推荐使用
//    val joinRDD: RDD[(String, (Int, Int))] = rdd1.join(rdd2)
//    joinRDD.collect().foreach(println)

    //  (a,1), (b,2)
    //  (a,(1,4)) , (b, (2,5))
    rdd1.map {
      case  (w, c) => {
        val i = map.getOrElse(w, 0)
        (w, (c, i))
      }
    }.collect().foreach(println)

    sc.stop()
  }
}
