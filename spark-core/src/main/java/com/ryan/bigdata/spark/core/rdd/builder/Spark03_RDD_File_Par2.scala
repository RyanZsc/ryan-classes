package com.ryan.bigdata.spark.core.rdd.builder

import org.apache.spark.{SparkConf, SparkContext}

object Spark03_RDD_File_Par2 {
  def main(args: Array[String]): Unit = {

    // TODO 准备环境
    // [*] 表示你当前设备可用的最大核数，不写就是单线程模拟单核
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    // TODO 创建RDD

    // 14byte / 2 = 7byte
    // 14 / 7 = 2(分区)

    /*
      1234567@@ => 12345678
      89@@      => 9101112
      0         => 13

      [0, 7]    => 1234567
      [7, 14]   => 890
     */

    // 如果数据源为多个文件，计算分区时以文件为单位进行分区
    val rdd = sc.textFile("datas/word.txt",2)

    rdd.saveAsTextFile("output")

    // TODO 关闭环境
    sc.stop()
  }
}
