package com.grady.transform.doublevalue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object TSubtract {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("subtract").setMaster("local[1]").set("spark.testing.memory", "500000000")
    val sc: SparkContext = new SparkContext(conf)

    val seqRdd: RDD[Int] = sc.makeRDD(1 to 5)
    val seqRdd2: RDD[Int] = sc.makeRDD(4 to 8)

    println("seqRdd - seqRdd2")
    seqRdd.subtract(seqRdd2).collect().foreach(println)
    println("seqRdd2 - seqRdd1")
    seqRdd2.subtract(seqRdd).collect().foreach(println)

  }
}
