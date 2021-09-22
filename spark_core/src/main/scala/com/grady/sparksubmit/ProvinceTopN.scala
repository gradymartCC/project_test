package com.grady.sparksubmit

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object ProvinceTopN {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("provinceTopN").setMaster("local[4]").set("spark.testing.memory", "500000000")
    val sc: SparkContext = new SparkContext(conf)

    val fileRdd: RDD[String] = sc.textFile(args(0))
    //    1516609143867 6 7 64 16
    //    时间戳 省份  城市  用户  广告

    // (省份，（广告，点击））
    val value: RDD[(String, List[(String, Int)])] = fileRdd.map(line => {
      val arr: Array[String] = line.split(" ")
      (arr(1) + "-" + arr(4), 1)
    })
      .reduceByKey(_ + _)
      .map {
        case (str, int) => {
          val arr: Array[String] = str.split("-")
          (arr(0), (arr(1), int))
        }
      }
      .groupByKey()
      .mapValues {
        data => {
          data.toList.sortBy(_._2).reverse
        }.take(3)
      }

    value.collect().foreach(println)

    sc.stop();
  }
}
