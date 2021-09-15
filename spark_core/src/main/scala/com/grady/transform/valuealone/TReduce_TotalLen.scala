package com.grady.transform.valuealone

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object TReduce_TotalLen {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("reduce").set("spark.testing.memory", "2147480000")
    val sc: SparkContext = new SparkContext(conf)

    val fileRDD: RDD[String] = sc.textFile("input/words")
    val lengthRdd: RDD[Int] = fileRDD.map(line => line.length)
    val totalLen: Int = lengthRdd.reduce(_ + _)

    print(totalLen)
  }

}
