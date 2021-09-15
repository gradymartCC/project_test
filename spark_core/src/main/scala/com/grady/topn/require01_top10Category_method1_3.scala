package com.grady.topn

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object require01_top10Category_method1_3 {
  def main(args: Array[String]): Unit = {
    //TODO 1 创建SparkConf配置文件,并设置App名称
    val conf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")
    //TODO 2 利用SparkConf创建sc对象
    val sc = new SparkContext(conf)
    //1.读取原始日志数据
    val actionRDD: RDD[String] = sc.textFile("input\\user_visit_action.txt")

    //2.将数据转换结构 (提前补0)
    // 如果数据是点击 : (品类ID,(1,0,0))
    // 如果数据是下单 : (品类ID,(0,1,0))
    // 如果数据是支付 : (品类ID,(0,0,1))
    val flatRDD: RDD[(String, (Int, Int, Int))] = actionRDD.flatMap {
      action => {
        val datas: Array[String] = action.split("_")
        if (datas(6) != "-1") {
          //点击的数据
          List((datas(6), (1, 0, 0)))
        } else if (datas(8) != "null") {
          //下单的数据
          val ids: Array[String] = datas(8).split(",")
          ids.map((_, (0, 1, 0)))
        } else if (datas(10) != "null") {
          //支付的数据
          val ids: Array[String] = datas(10).split(",")
          ids.map((_, (0, 0, 1)))
        } else {
          Nil
        }
      }
    }

    //3.将相同的品类ID的数据进行分组聚合 reduceByKey
    val reduceRDD: RDD[(String, (Int, Int, Int))] = flatRDD.reduceByKey {
      (t1, t2) => {
        (t1._1 + t2._1, t1._2 + t2._2, t1._3 + t2._3)
      }
    }

    //倒序排序取前10
    val result: Array[(String, (Int, Int, Int))] = reduceRDD.sortBy(_._2, false).take(10)

    //4.将结果采集到控制台打印输出
    result.foreach(println)

    //TODO 3 关闭资源
    sc.stop()
  }
}
