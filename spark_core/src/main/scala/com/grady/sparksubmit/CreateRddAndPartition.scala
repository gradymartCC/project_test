package com.grady.sparksubmit

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object CreateRddAndPartition {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("create and partition").setMaster("local[1]").set("spark.testing.memory","500000000")
    val sc: SparkContext = new SparkContext(conf)

    val p2Rdd: RDD[Int] = sc.makeRDD(Array(1, 2, 3, 4), 2)

    val fileRdd: RDD[String] = sc.textFile("input/words")

    p2Rdd.mapPartitionsWithIndex((index,items) => items.map((index,_)))
              .collect().foreach(println)

    fileRdd.mapPartitionsWithIndex((index,items) => items.map((index,_)))
              .collect().foreach(println)
  }
}
