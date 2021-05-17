package com.ryan.bigdata.spark.core.rdd.builder

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_Memory_Par {
  def main(args: Array[String]): Unit = {

    // TODO 准备环境
    // [*] 表示你当前设备可用的最大核数，不写就是单线程模拟单核
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
//    sparkConf.set("spark.default.parallelism","3")
    val sc = new SparkContext(sparkConf)

    // TODO 创建RDD
    // RDD的并行度 & 分区
    // makeRDD 方法可以传递第二个参数，这个参数表示分区的数量
    // 第二个参数若不传参数，默认值为defaultParallelism（默认为并行度）
    //    scheduler.conf.getInt("spark.default.parallelism", totalCores)
    //   spark 在默认情况下，从配置对象中获取配置参数：spark.default.parallelism
    //   如果获取不到，则使用 totalCores ，此属性取值为当前环境的可用最大核数
    val rdd = sc.makeRDD(List(1,2,3,4))

    // 将处理的数据保存成分区文件
    rdd.saveAsTextFile("output")


    // TODO 关闭环境
    sc.stop()
  }
}
