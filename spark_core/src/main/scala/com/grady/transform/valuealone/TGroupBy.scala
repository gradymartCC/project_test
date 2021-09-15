package com.grady.transform.valuealone

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object TGroupBy {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("group by").setMaster("local[1]").set("spark.testing.memory", "500000000")
    val sc: SparkContext = new SparkContext(conf)

    // 将奇偶数放在两个数组中
    val arrayRdd: RDD[Int] = sc.makeRDD(Array(1, 2, 3, 4,5,6,7,8,9,10))

    val groupByRdd: RDD[(Int, Iterable[Int])] = arrayRdd.groupBy(int => int % 3)

    groupByRdd.collect().foreach(println)

  }

}
