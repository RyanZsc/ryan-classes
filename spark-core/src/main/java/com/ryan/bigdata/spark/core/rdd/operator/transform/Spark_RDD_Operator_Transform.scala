package com.ryan.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}


object Spark_RDD_Operator_Transform {

  // TODO map
  def rdd_map(sc: SparkContext): Unit = {
    val rdd = sc.makeRDD(List(1,2,3,4))

    val mapRDD: RDD[Int] = rdd.map(_*2)

    mapRDD.collect().foreach(println)
  }

  // TODO map_par
  def rdd_map_par(sc: SparkContext): Unit = {
    val rdd = sc.makeRDD(List(1,2,3,4))

    val mapRDD: RDD[Int] = rdd.map(num => {
      println("<<<<<<<<<mapRDD: " + num)
      num
    })

    val mapRDD1: RDD[Int] = mapRDD.map(num => {
      println("《《《《《mapRDD1: " + num)
      num
    })

    mapRDD1.collect()
  }

  // TODO map_partition
  def rdd_map_part(sc: SparkContext): Unit = {
    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4),2)
    rdd.saveAsTextFile("output")

    val mapRDD: RDD[Int] = rdd.map(_*2)
    mapRDD.saveAsTextFile("output1")
  }

  // TODO map_split
  def rdd_map_split(sc: SparkContext): Unit = {
    val rdd: RDD[String] = sc.textFile("datas/apache.log")

    val mapRDD: RDD[String] = rdd.map(_.split(" ")(6))
    mapRDD.collect().foreach(println)
  }

  // TODO rdd_mappartition
  def rdd_mappartition(sc: SparkContext): Unit = {
    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4),2)

    val mapRDD: RDD[Int] = rdd.mapPartitions(iter=>{
      println("<<<<<<")
      iter.map(_*2)
    })

    mapRDD.collect().foreach(println)
  }

  // TODO rdd_mappartition_max
  def rdd_mappartition_max(sc: SparkContext): Unit = {
    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4),2)

    val mapRDD: RDD[Int] = rdd.mapPartitions(iter => {
      List(iter.max).iterator
    })
    mapRDD.collect().foreach(println)
  }

  // TODO mapPartitionsWithIndex
  def rdd_mapPartitionsWithIndex(sc: SparkContext): Unit = {
    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4),2)

    val mpiRDD: RDD[Int] = rdd.mapPartitionsWithIndex((index, iter) => {
      if (index == 0) {
        iter
      } else {
        Nil.iterator
      }
    })
    mpiRDD.collect().foreach(println)
  }

  // TODO mapPartitionsWithIndex2
  def rdd_mapPartitionsWithIndex2(sc: SparkContext): Unit = {
    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4))

    val mpiRDD = rdd.mapPartitionsWithIndex((index, iter) => {
      iter.map(num => {
        (index, num)
      })
    })
    mpiRDD.collect().foreach(println)
  }

  // TODO flatMap
  def rdd_flatMap(sc: SparkContext): Unit = {
    val rdd: RDD[List[Int]] = sc.makeRDD(List(
      List(1, 2), List(3, 4)
    ))

    val flatRDD: RDD[Int] = rdd.flatMap(
      List => List
    )

    flatRDD.collect().foreach(println)
  }

  // TODO flatMap
  def rdd_flatMap2(sc: SparkContext): Unit = {
    val rdd = sc.makeRDD(List(
      "Hello Spark", "Hello Scala"
    ))

    rdd.flatMap(_.split(" ")).collect().foreach(println)
  }

  // TODO flatMap
  def rdd_flatMap3(sc: SparkContext): Unit = {
    val rdd = sc.makeRDD(List(List(1,2),3,List(4,5)))


    rdd.flatMap {
      case list: List[_] => list
      case data => List(data)
    }.collect().foreach(println)

  }

  // TODO glom
  def rdd_glom(sc: SparkContext): Unit = {
    val rdd = sc.makeRDD(List(1,2,3,4),2)

    rdd.glom().collect().foreach(data=> println(data.mkString(",")))
  }

  // TODO glom2
  def rdd_glom2(sc: SparkContext): Unit = {
    val rdd = sc.makeRDD(List(1,2,3,4),2)

    println(rdd.glom().map(_.max).collect().sum)
  }

  // TODO groupBy
  def rdd_groupBy(sc: SparkContext): Unit = {
    val rdd = sc.makeRDD(List(1,2,3,4),2)

    rdd.groupBy(_%2).collect().foreach(println)
  }

  // TODO groupBy
  def rdd_groupBy2(sc: SparkContext): Unit = {
    val rdd = sc.makeRDD(List("Hello", "Spark", "Scala", "Hadoop"),2)

    rdd.groupBy(_.charAt(0)).collect().foreach(println)
  }

  // TODO groupBy3
  def rdd_groupBy3(sc: SparkContext): Unit = {
    val rdd = sc.textFile("datas/apache.log")

    rdd.map(line => (line.split(" ")(3).split(":")(1),1)).groupBy(_._1).map(line=>(line._1,line._2.size)).collect().foreach(println)
  }

  // TODO filter
  def rdd_filter(sc: SparkContext): Unit = {
    sc.makeRDD(List(1,2,3,4)).filter(_%2!=0).collect().foreach(println)
  }

  // TODO filter2
  def rdd_filter2(sc: SparkContext): Unit = {
    sc.textFile("datas/apache.log").filter(_.split(" ")(3).split(":")(0)=="17/05/2015")
      .collect().foreach(println)
  }

  // TODO sample
  def rdd_sample(sc: SparkContext): Unit = {
    println(sc.makeRDD(List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)).sample(withReplacement = false, 0.4).collect().mkString(","))
  }

  // TODO distinct
  def rdd_distinct(sc: SparkContext): Unit = {
    sc.makeRDD(List(1,2,3,4,1,2,3,4)).distinct().collect().foreach(println)
  }

  // TODO coalesce
  def rdd_coalesce(sc: SparkContext): Unit = {
    sc.makeRDD(List(1,2,3,4,5,6),3).coalesce(2,shuffle = true).saveAsTextFile("output")
  }

  // TODO repartition
  def rdd_repartition(sc: SparkContext): Unit = {
    sc.makeRDD(List(1,2,3,4,5,6),2).repartition(3).saveAsTextFile("output")
  }

  // TODO sortBy
  def rdd_sortBy(sc: SparkContext): Unit = {
    sc.makeRDD(List(6,2,4,5,3,1),2).sortBy(n=>n).collect().foreach(println)
  }

  // TODO sortBy2
  def rdd_sortBy2(sc: SparkContext): Unit = {
    sc.makeRDD(List(("1",1),("11",2),("2",3)),2).sortBy(_._1.toInt,ascending = false)
      .collect().foreach(println)
  }

  // TODO key 双value类型
  def rdd_key_values(sc: SparkContext): Unit = {
    val rdd1 = sc.makeRDD(List(1,2,3,4))
    val rdd2 = sc.makeRDD(List(3,4,5,6))
    val rdd3 = sc.makeRDD(List("3","4","5","6"))

    // 差集
    println(rdd1.intersection(rdd2).collect().mkString(","))
    // 并集
    println(rdd1.union(rdd2).collect().mkString(","))
    // 差集
    println(rdd1.subtract(rdd2).collect().mkString(","))
    // 拉链
    println(rdd1.zip(rdd2).collect().mkString(","))
    println(rdd1.zip(rdd3).collect().mkString(","))

  }

  // TODO  key 双value类型
  def rdd_rdd_key_values2(sc: SparkContext): Unit = {
    val rdd1 = sc.makeRDD(List(1,2,3,4,5,6),2)
    val rdd2 = sc.makeRDD(List(3,4,5,6),2)
    // 拉链
    println(rdd1.zip(rdd2).collect().mkString(","))
  }

  // TODO 算子 -(key value类型) partitionBy
  def rdd_partitionBy(sc: SparkContext): Unit = {
    sc.makeRDD(List(1,2,3,4),2).map((_,1)).partitionBy(new HashPartitioner(2))
      .saveAsTextFile("output")
  }

  // TODO reduceByKey
  def rdd_reduceByKey(sc: SparkContext): Unit = {
    sc.makeRDD(List(
      ("a",1),("a",2),("a",3),("b",4)
    )).reduceByKey(_+_).collect().foreach(println)
  }

  // TODO groupByKey
  def rdd_groupByKey(sc: SparkContext): Unit = {
    val rdd = sc.makeRDD(List(
      ("a", 1), ("a", 2), ("a", 3), ("b", 4)
    ))

    rdd.groupByKey().collect().foreach(println)
    rdd.groupBy(_._1).collect().foreach(println)
  }

  // TODO aggregateByKey
  def rdd_aggregateByKey(sc: SparkContext): Unit = {
    val rdd = sc.makeRDD(List(
      ("a",1),("a",2),("a",3),("a",4)
    ),2)

    rdd.aggregateByKey(0)(
      (x,y) => math.max(x,y),
      _+_
    ).collect().foreach(println)
  }

  // TODO foldByKey
  def rdd_foldByKey(sc: SparkContext): Unit = {
    val rdd = sc.makeRDD(List(
      ("a",1),("a",2),("a",3),("a",4)
    ),2)

    rdd.aggregateByKey(0)(_+_,_+_).collect().foreach(println)
    rdd.foldByKey(0)(_+_).collect().foreach(println)
  }

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

//    rdd_map(sc)
//    rdd_map_par(sc)
//    rdd_map_part(sc)
//    rdd_map_split(sc)
//    rdd_mappartition(sc)
//    rdd_mappartition_max(sc)
//    rdd_mapPartitionsWithIndex(sc)
//    rdd_mapPartitionsWithIndex2(sc)
//    rdd_flatMap(sc)
//    rdd_flatMap2(sc)
//    rdd_flatMap3(sc)
//    rdd_glom(sc)
//    rdd_glom2(sc)
//    rdd_groupBy(sc)
//    rdd_groupBy2(sc)
//    rdd_groupBy3(sc)
//    rdd_filter(sc)
//    rdd_filter2(sc)
//    rdd_sample(sc)
//    rdd_distinct(sc)
//    rdd_coalesce(sc)
//    rdd_repartition(sc)
//    rdd_sortBy(sc)
//    rdd_sortBy2(sc)
//    rdd_key_values(sc)
//    rdd_rdd_key_values2(sc)
//    rdd_partitionBy(sc)
//    rdd_reduceByKey(sc)
//    rdd_groupByKey(sc)
//    rdd_aggregateByKey(sc)
    rdd_foldByKey(sc)


    sc.stop()
  }
}

