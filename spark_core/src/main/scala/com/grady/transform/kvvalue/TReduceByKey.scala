package com.grady.transform.kvvalue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object TReduceByKey {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().set("spark.testing.memory", "500000000").setMaster("local[1]").setAppName("reduceByKey")
    val sc: SparkContext = new SparkContext(conf)

    val listRdd: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("b", 5), ("a", 5), ("b", 2)))

    listRdd.reduceByKey(_+_).collect().foreach(println)
  }

}
