package com.ryan.bigdata.spark.core.rdd.persist

import org.apache.spark.{SparkConf, SparkContext}

object Spark06_RDD_Persist {
  def main(args: Array[String]): Unit = {

    // cache : 将数据临时存储在内存中进行数据重用
    //          会在血缘关系中添加新的依赖。一旦出现问题，可以从头读取数据
    // persist : 将数据临时存储在磁盘文件中进行数据重用
    //            涉及到磁盘IO，性能较低，但是数据安全
    //            如果作业执行完毕，临时保存的数据文件就会丢失
    // checkpoint : 将数据长久的保存在磁盘文件中进行数据重用
    //            涉及到磁盘IO，性能较低，但是数据安全
    //            为了保证数据安全，所以一般情况下会对执行作业
    //            为了能够提高效率，一般情况下，是需要和cache联合使用
    //            执行过程中，会切断血缘关系，重新建立新的血缘关系
    //            checkpoint 等同于改变数据源
    val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)
    sc.setCheckpointDir("cp")

    val list = List("Hello Scala", "Hello Spark")

    val rdd = sc.makeRDD(list)

    val flatRDD = rdd.flatMap(_.split(" "))

    val mapRDD = flatRDD.map(word=>{
      (word,1)
    })
//    mapRDD.cache()
    mapRDD.checkpoint()
    println(mapRDD.toDebugString)

    val reduceRDD = mapRDD.reduceByKey(_+_)

    reduceRDD.collect().foreach(println)

    println("**************************************")
    println(mapRDD.toDebugString)

    /*
      RDD对象的持久化操作不一定是为了重用
      在数据执行较长，或数据比较重要的场合也可以采用持久化操作
     */

    /*
      ssh
      4wmgjgqv.dongtaiyuming.net:21880
      127.0.0.1:22

      http（开了tomcat可测试）
      nnbvpqp.nat.ipyingshe.com:19817
      127.0.0.1:8080
     */

    sc.stop()
  }
}
