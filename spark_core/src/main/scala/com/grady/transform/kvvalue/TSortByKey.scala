package com.grady.transform.kvvalue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object TSortByKey {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("sortByKey").set("spark.testing.memory", "500000000")

    val sc: SparkContext = new SparkContext(conf)
    val listRdd: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("b", 5), ("c", 5), ("d", 2)))

    val sortRdd: RDD[(String, Int)] = listRdd.sortByKey(ascending = false)
    sortRdd.collect().foreach(println)
  }
}
