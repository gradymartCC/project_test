package com.grady.transform.valuealone

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object TSortBy {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[1]").setAppName("sort by").set("spark.testing.memory", "500000000")
    val sc: SparkContext = new SparkContext(conf)

    val sortRdd: RDD[(String, Int)] = sc.textFile("input/words")
      .flatMap(line => line.split(" "))
      .map((_, 1))
      .reduceByKey(_ + _)
      .sortBy(_._2,false)//第二个参数true正序，false逆序

    sortRdd.collect().foreach(println)
  }

}
