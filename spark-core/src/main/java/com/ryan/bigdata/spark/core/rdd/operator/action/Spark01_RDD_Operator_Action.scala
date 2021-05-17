package com.ryan.bigdata.spark.core.rdd.operator.action

import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_Operator_Action {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    val rdd = sc.makeRDD(List(1,2,3,4))

    // TODO 行动算子
    // 所谓的行动算子就是 触发作业(Job)执行的方法
    // 底层代码调用的是环境对象的 runJob 方法
    // 底层代码中会创建 ActiveJob ，并提交执行
    rdd.collect()

    sc.stop()
  }
}
