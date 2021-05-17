package com.ryan.bigdata.spark.core.mytest

import org.apache.spark.sql.SparkSession

object Test01 {
  def main(args: Array[String]): Unit = {
    val logFile = "D:\\Spark\\spark-3.0.0-bin-hadoop3.2\\bin\\input\\word.txt"
    val spark = SparkSession.builder.appName("Sample").getOrCreate()
    val logData = spark.read.textFile(logFile).cache()
    val numAs = logData.filter(_.contains("a")).count()
    val numBs = logData.filter(_.contains("b")).count()

    println(s"Lines with a: $numAs, lines with b: $numBs")
    spark.stop()
  }
}
