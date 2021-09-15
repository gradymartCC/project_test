package com.grady.transform.valuealone

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object TCoalesce {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[1]").setAppName("coalesce").set("spark.testing.memory", "500000000")
    val sc: SparkContext = new SparkContext(conf)

    // 创建一个RDD 4个分区
    val p6Rdd: RDD[Int] = sc.makeRDD(Array(1, 2, 3, 4, 5, 6), 6)

    // coalesce缩减分区
    val coalRdd: RDD[Int] = p6Rdd.coalesce(2)
    coalRdd.mapPartitionsWithIndex((index,items) => items.map((index,_)))
              .collect().foreach(println)

    println("-------")
    // 增加分区，没有走shuffle就没有意义，false默认，true走shuffle
    val coal6Rdd: RDD[Int] = coalRdd.coalesce(6)
//    val coal6Rdd: RDD[Int] = coalRdd.coalesce(6,true)
    coal6Rdd.mapPartitionsWithIndex((index,items) => items.map((index,_)))
              .collect().foreach(println)
  }

}
