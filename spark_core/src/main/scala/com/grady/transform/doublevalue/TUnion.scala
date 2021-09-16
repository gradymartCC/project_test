package com.grady.transform.doublevalue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object TUnion {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[1]").setAppName("union").set("spark.testing.memory", "500000000")
    val sc: SparkContext = new SparkContext(conf)

    val oneRdd: RDD[Int] = sc.makeRDD(1 to 3)
    val twoRdd: RDD[Int] = sc.makeRDD(2 to 4)

    oneRdd.union(twoRdd).collect().foreach(println)
  }
}
