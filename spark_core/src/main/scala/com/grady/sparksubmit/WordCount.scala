package com.grady.sparksubmit

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("WordCount").setMaster("local[*]")
//    conf.set("spark.testing.memory", "2147480000")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val flieRDD: RDD[String] = sc.textFile(args(0))
//    val flieRDD: RDD[String] = sc.textFile("input/words")

    val result: RDD[(String, Int)] = flieRDD.flatMap(line => {
      line.split(" ")
    })
      .map((_, 1))
      .reduceByKey(_ + _)

    result.collect().foreach(println)
    //4.关闭连接
    sc.stop()
  }
}

// hadoop fs -put ${execution.project_share_dir}/words /user/root/grady_test/input
// hadoop fs -ls /user/root/grady_test/input

// Input path does not exist: hdfs://ip-172-31-29-32.cn-north-1.compute.internal:8020/user/root/input/words