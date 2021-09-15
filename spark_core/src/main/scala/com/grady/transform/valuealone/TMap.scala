package com.grady.transform.valuealone

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object TMap {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("transform map").setMaster("local[*]").set("spark.testing.memory", "2147480000")
    val sc: SparkContext = new SparkContext(conf)

    val fileRDD: RDD[String] = sc.textFile("input/words")
    val mapRDD: RDD[Array[String]] = fileRDD.map(line => {
      line.split(" ")
    })

    mapRDD.collect().foreach(println)
  }

}
