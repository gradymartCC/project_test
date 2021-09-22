package com.grady.transform.kvvalue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Tcogroup {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("cogroup").setMaster("local[4]").set("spark.testing.memory", "500000000")
    val sc: SparkContext = new SparkContext(conf)

    //3.1 创建第一个RDD
    val rdd: RDD[(Int, String)] = sc.makeRDD(Array((1, "a"), (2, "b"), (3, "c")))

    //3.2 创建第二个pairRDD
    val rdd1: RDD[(Int, Int)] = sc.makeRDD(Array((1, 4), (2, 5), (4, 6)))

    rdd.cogroup(rdd1).collect().foreach(println)
//    (4,(CompactBuffer(),CompactBuffer(6)))
//    (1,(CompactBuffer(a),CompactBuffer(4)))
//    (2,(CompactBuffer(b),CompactBuffer(5)))
//    (3,(CompactBuffer(c),CompactBuffer()))
  }
}
