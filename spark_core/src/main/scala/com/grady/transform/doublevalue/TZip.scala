package com.grady.transform.doublevalue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object TZip {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("zip").set("spark.testing.memory", "500000000")
    val sc: SparkContext = new SparkContext(conf)

    //zip 同分区同元素个数
    val minRdd: RDD[Int] = sc.makeRDD(1 to 3)
    val maxRdd: RDD[Int] = sc.makeRDD(4 to 6)
//    val maxRdd: RDD[Int] = sc.makeRDD(4 to 7)
    //org.apache.spark.SparkException:
    // Can only zip RDDs with same number of elements in each partition

    // 验证集合获取rdd的默认分区数是与local[1]有关，还是计算机最大核数
    maxRdd.mapPartitionsWithIndex((index,data)=>data.map((_,index)))
      .collect()
      .foreach(println)//与你设置local[1]核数有关

    val zipRdd: RDD[(Int, Int)] = maxRdd.zip(minRdd)

    zipRdd.collect().foreach(println)
  }
}
