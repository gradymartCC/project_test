package com.grady.transform.kvvalue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object TCombineByKey {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("combineByKey").set("spark.testing.memory", "500000000")
    val sc: SparkContext = new SparkContext(conf)

    val listRdd: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("b", 5), ("a", 5), ("b", 2)))

    val comRdd: RDD[(String, (Int, Int))] = listRdd.combineByKey(t => (t, 1), (tt: (Int, Int), v) => (tt._1 + v, tt._2 + 1), (a: (Int, Int), b: (Int, Int)) => (a._1 + b._1, a._2 + b._2))

    comRdd.collect().foreach(println)

    comRdd.map{case(key,value)=>(key,value._1/value._2)}.collect().foreach(println)
  }

}
