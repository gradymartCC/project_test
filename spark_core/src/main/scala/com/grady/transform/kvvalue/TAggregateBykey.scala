package com.grady.transform.kvvalue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object TAggregateBykey {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("aggregateByKey").setMaster("local[*]").set("spark.testing.memory", "500000000")
    val sc: SparkContext = new SparkContext(conf)

    val listRdd: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("b", 5), ("a", 5), ("b", 2)))

    val aggRdd: RDD[(String, Int)] = listRdd.aggregateByKey(0)(_ + _, _ + _)

    aggRdd.collect().foreach(println)
  }

}
