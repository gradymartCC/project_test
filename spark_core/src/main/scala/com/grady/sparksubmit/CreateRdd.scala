package com.grady.sparksubmit

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object CreateRdd {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[1]").setAppName("create rdd").set("spark.testing.memory", "500000000")
    val sc: SparkContext = new SparkContext(conf)

    // 文件系统
    val value: RDD[String] = sc.textFile("input/words")

    // 集合
    val setRdd: RDD[Int] = sc.makeRDD(Array(1, 2, 3, 4))

    // rdd转换
    val add4Rdd: RDD[Int] = setRdd.map(_ + 4)

    add4Rdd.collect().foreach(println)
    value.collect().foreach(println)
  }

}
