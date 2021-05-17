package com.ryan.bigdata.spark.core.rdd.persist

import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object Spark04_RDD_Persist {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)
    sc.setCheckpointDir("cp")

    val list = List("Hello Scala", "Hello Spark")

    val rdd = sc.makeRDD(list)

    val flatRDD = rdd.flatMap(_.split(" "))

    val mapRDD = flatRDD.map(word=>{
      println("@@@@@@@@@@")
      (word,1)
    })
    // checkpoint 需要落盘，需要指定检查点保存路径
    // 检查点路径保存的文件，当作业执行完成后，不会被删除
    // 一般保存路径都是在分布式存储系统中：HDFS
    mapRDD.checkpoint()

    val reduceRDD = mapRDD.reduceByKey(_+_)

    reduceRDD.collect().foreach(println)

    println("**************************************")

    val groupRDD = mapRDD.groupByKey()
    groupRDD.collect().foreach(println)

    /*
      RDD对象的持久化操作不一定是为了重用
      在数据执行较长，或数据比较重要的场合也可以采用持久化操作
     */

    sc.stop()
  }
}
