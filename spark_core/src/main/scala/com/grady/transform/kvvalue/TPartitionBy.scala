package com.grady.transform.kvvalue

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, RangePartitioner, SparkConf, SparkContext}

object TPartitionBy {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[1]").setAppName("partition by").set("spark.testing.memory", "500000000")
    val sc: SparkContext = new SparkContext(conf)

    // partition by
    val arrRdd: RDD[(String, String)] = sc.makeRDD(Array(("a", "A"), ("b", "B"), ("c", "C"),("a", "A")))

//    val parRdd: RDD[(String, String)] = arrRdd.partitionBy(new RangePartitioner(3, arrRdd))
/*    (0,(a,A))
    (0,(a,A))
    (1,(b,B))
    (2,(c,C))*/

    val parRdd: RDD[(String, String)] = arrRdd.partitionBy(new HashPartitioner(3))
/*    (0,(c,C))
    (1,(a,A))
    (1,(a,A))
    (2,(b,B))*/

    parRdd.mapPartitionsWithIndex((index,items) => items.map((index,_)))
              .collect().foreach(println)
  }

}
