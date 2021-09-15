package com.grady.transform.valuealone

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object TFilter {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("filter").setMaster("local[1]").set("spark.testing.memory", "500000000")
    val sc: SparkContext = new SparkContext(conf)

    val arrayRdd: RDD[Int] = sc.makeRDD(Array(1, 2, 3, 4, 5))

    val ouRdd: RDD[Int] = arrayRdd.filter(_ % 2 != 0)

    ouRdd.collect().foreach(println)
  }

}
