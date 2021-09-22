package com.grady.transform.kvvalue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object TFoldByKey {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("foldByKey").set("spark.testing.memory", "500000000")
    val sc: SparkContext = new SparkContext(conf)

    val listRdd: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("b", 5), ("a", 5), ("b", 2)))

    val foldRdd: RDD[(String, Int)] = listRdd.foldByKey(1)(_ + _)

    foldRdd.collect().foreach(println)
  }
}
