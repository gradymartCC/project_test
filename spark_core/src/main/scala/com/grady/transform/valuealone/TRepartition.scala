package com.grady.transform.valuealone

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object TRepartition {//repartition
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("repartition").setMaster("local[1]").set("spark.testing.memory", "500000000")
    val sc: SparkContext = new SparkContext(conf)

    val listRdd: RDD[Int] = sc.makeRDD(List(1, 3, 4, 5, 6, 9),4)
    val rep2Rdd: RDD[Int] = listRdd.repartition(2)

    val rep6Rdd: RDD[Int] = listRdd.repartition(6)

    rep2Rdd.mapPartitionsWithIndex((index,items) => items.map((index,_)))
              .collect().foreach(println)

    println("-----------")

    rep6Rdd.mapPartitionsWithIndex((index,items) => items.map((index,_)))
              .collect().foreach(println)
  }

}
