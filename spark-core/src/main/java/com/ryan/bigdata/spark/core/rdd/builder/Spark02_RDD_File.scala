package com.ryan.bigdata.spark.core.rdd.builder

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_RDD_File {
  def main(args: Array[String]): Unit = {

    // TODO 准备环境
    // [*] 表示你当前设备可用的最大核数，不写就是单线程模拟单核
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    // TODO 创建RDD
    // 从文件中创建RDD，将文件中的数据作为处理的数据源
    // path 路径默认以当前环境的根目录为基准，可以绝对或相对路径
//    val rdd: RDD[String] = sc.textFile("datas/1.txt")
    // path 可以是文件的具体路径，也可以是目录的名称
//    val rdd: RDD[String] = sc.textFile("datas")
    // path 路径还可以使用通配符
    val rdd: RDD[String] = sc.textFile("datas/1*.txt")
    //path 还可以是分布式系统路径：HDFS
//    sc.textFile("hdfs://192.168.100.101:9000/test.txt")

    rdd.collect().foreach(println)

    // TODO 关闭环境
    sc.stop()
  }
}
