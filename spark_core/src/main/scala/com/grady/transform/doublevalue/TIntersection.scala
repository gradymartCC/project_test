package com.grady.transform.doublevalue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object TIntersection {//intersection
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[1]").setAppName("intersection").set("spark.testing.memory", "500000000")
    val sc: SparkContext = new SparkContext(conf)

    val arrRdd: RDD[Int] = sc.makeRDD(Array(1, 3, 5, 7, 9))
    val listRdd: RDD[Int] = sc.makeRDD(List(2, 4, 6, 8,9))

    val intersetionRdd: RDD[Int] = arrRdd.intersection(listRdd)

    intersetionRdd.collect().foreach(println)
  }
}
