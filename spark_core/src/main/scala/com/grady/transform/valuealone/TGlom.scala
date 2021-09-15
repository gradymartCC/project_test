package com.grady.transform.valuealone

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object TGlom {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[1]").setAppName("glom").set("spark.testing.memory", "500000000")
    val sc: SparkContext = new SparkContext(conf)

    val maxRdd: RDD[Int] = sc.makeRDD(Array(1, 2, 3, 4), 2)
      .glom()
      .map(_.max)

    // maxBy
    val maxByRdd: RDD[Int] = sc.makeRDD(Array(2, 4, 6, 8), 2)
      .glom()
      .map(_.maxBy(f => f))

    maxRdd.collect().foreach(println)
    maxByRdd.collect().foreach(println)
  }
}
