package com.grady.transform.valuealone

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object TDistinct {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("distinct").setMaster("local[1]").set("spark.testing.memory", "500000000")
    val sc: SparkContext = new SparkContext(conf)

    val fileRdd: RDD[String] = sc.textFile("input/words")

    val disRdd: RDD[String] = fileRdd.flatMap(line => {
      line.split(" ")
    })
      .distinct()// distinct(2) 两个并发做处理

    disRdd.collect().foreach(println)
  }

}
