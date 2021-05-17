package com.ryan.bigdata.spark.core.wc

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object Spark03_WordCount {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)

    wordcount8(sc)

    sc.stop()
  }

  def wordcount1(sc: SparkContext): Unit = {
    val rdd = sc.makeRDD(List("Hello Scala","Hello Spark"))
    val groupRDD = rdd.flatMap(_.split(" ")).groupBy(word=>word)
    val wordCount = groupRDD.mapValues(iter=>iter.size)
    wordCount.collect().foreach(println)
  }

  // groupByKey  有shuffle 性能不高
  def wordcount2(sc: SparkContext): Unit = {
    val rdd = sc.makeRDD(List("Hello Scala","Hello Spark"))
    val group = rdd.flatMap(_.split(" ")).map((_,1)).groupByKey()
    group.mapValues(iter=>iter.size).collect().foreach(println)
  }

  // reduceByKey
  def wordcount3(sc: SparkContext): Unit = {
    val rdd = sc.makeRDD(List("Hello Scala","Hello Spark"))
    val group = rdd.flatMap(_.split(" ")).map((_,1))
    group.reduceByKey(_+_).collect().foreach(println)
  }

  // aggregateByKey
  def wordcount4(sc: SparkContext): Unit = {
    val rdd = sc.makeRDD(List("Hello Scala","Hello Spark"))
    val group = rdd.flatMap(_.split(" ")).map((_,1))
    group.aggregateByKey(0)(_+_,_+_).collect().foreach(println)
  }

  // foldByKey
  def wordcount5(sc: SparkContext): Unit = {
    val rdd = sc.makeRDD(List("Hello Scala","Hello Spark"))
    val group = rdd.flatMap(_.split(" ")).map((_,1))
    group.foldByKey(0)(_+_).collect().foreach(println)
  }

  // combineByKey
  def wordcount6(sc: SparkContext): Unit = {
    val rdd = sc.makeRDD(List("Hello Scala","Hello Spark"))
    val group = rdd.flatMap(_.split(" ")).map((_,1))
    group.combineByKey(
      v=>v,
      (x:Int,y) => x+y,
      (x:Int,y:Int) => x+y
    ).collect().foreach(println)
  }

  // countByKey
  def wordcount7(sc: SparkContext): Unit = {
    val rdd = sc.makeRDD(List("Hello Scala","Hello Spark"))
    val group = rdd.flatMap(_.split(" ")).map((_,1))
    println(group.countByKey())
  }

  // countByValue
  def wordcount8(sc: SparkContext): Unit = {
    val rdd = sc.makeRDD(List("Hello Scala","Hello Spark"))
    val group = rdd.flatMap(_.split(" "))
    println(group.countByValue())
  }

  // reduce aggregate fold
  def wordcount91011(sc: SparkContext): Unit = {
    val rdd = sc.makeRDD(List("Hello Scala","Hello Spark"))
    val group = rdd.flatMap(_.split(" "))

    // 【(word, count), (word, count)】
    // word => Map[(word,1)]
    val mapWord = group.map(
      word => {
        mutable.Map[String, Long]((word, 1))
      }
    )
    val wordCount = mapWord.reduce(
      (map1, map2) =>
        {map2.foreach{
          case (word, count) =>
            val newCount = map1.getOrElse(word, 0L) + count
            map1.update(word, newCount)
        }
        map1
      }
    )
    println(wordCount)

  }
}
