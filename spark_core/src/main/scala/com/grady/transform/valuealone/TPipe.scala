package com.grady.transform.valuealone

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object TPipe {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[1]").setAppName("pipe").set("spark.testing.memory", "500000000")
    val sc: SparkContext = new SparkContext(conf)

    val fileRdd: RDD[String] = sc.textFile("input/words")
    // 管道，针对每个分区，都执行一个shell脚本，返回输出的RDD。
    fileRdd.pipe("")

    // eg：pipe.sh
    /*
    #!/bin/sh
      echo "AA"
    while read LINE; do
      echo ">>>"${LINE}
    done
    */
    /*结果
        scala> rdd.pipe("/opt/module/spark/pipe.sh").collect()
        res18: Array[String] = Array(AA, >>>hi, >>>Hello, >>>how, >>>are, >>>you)
        */
  }
}
