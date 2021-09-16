package com.grady.sparksubmit

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object ToAndUtil {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[1]").setAppName("to and util").set("spark.testing.memory", "500000000")
    val sc: SparkContext = new SparkContext(conf)

    val toRdd: RDD[Int] = sc.makeRDD(1 to 3)

    toRdd.collect().foreach(println)

//    sc.makeRDD(1 nutil 4)
  }
}
