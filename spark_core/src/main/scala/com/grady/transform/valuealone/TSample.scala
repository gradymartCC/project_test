package com.grady.transform.valuealone

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object TSample {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[1]").setAppName("sample").set("spark.testing.memory", "500000000")
    val sc: SparkContext = new SparkContext(conf)

    val listRdd: RDD[Int] = sc.makeRDD(List(1, 2, 4, 5, 6, 7))

    // true放回，false不放回（伯努利算法）
    val sampleRdd: RDD[Int] = listRdd.sample(true, 0.5)

    // 第二个参数：抽取的几率，范围在[0,1]之间,0：全不取；1：全取；
    val sampleRdd2: RDD[Int] = listRdd.sample(true, 0.1)

    // 第三个参数：随机数种子
    val sampleRdd3: RDD[Int] = listRdd.sample(false, 0.5)

    val sampleRdd4: RDD[Int] = listRdd.sample(false, 0.1)

    sampleRdd.collect().foreach(println)
    println("----------")
    sampleRdd2.collect().foreach(println)

    println("==========")

    sampleRdd3.collect().foreach(println)
    println("----------")
    sampleRdd4.collect().foreach(println)
  }
}
