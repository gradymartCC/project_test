package com.grady.sparksubmit

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WCWithGroupBy {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[1]").setAppName("group by with wc").set("spark.testing.memory", "500000000")
    val sc: SparkContext = new SparkContext(conf)

    val fileRdd: RDD[String] = sc.textFile("input/words")

    val result: RDD[(String, Int)] = fileRdd.flatMap(line => {
      line.split(" ")
    })
      .map((_, 1))
      .groupBy(_._1)
      .map {
        case (k, iter) => {
          (k, iter.size)
        }
      }
    result.collect().foreach(println)
  }

}
