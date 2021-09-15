package com.grady.topn

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object require01_top10Category_method1_2 {
  def main(args: Array[String]): Unit = {
    //TODO 1 创建SparkConf配置文件,并设置App名称
    val conf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")
    //TODO 2 利用SparkConf创建sc对象
    val sc = new SparkContext(conf)
    //1.读取原始日志数据
    val actionRDD: RDD[String] = sc.textFile("input\\user_visit_action.txt")

    //问题1:actionRDD在下面用到了多次,最好缓存一下
    actionRDD.cache()

    //2.统计品类的点击数量 (品类ID,点击数量)
    val clickActionRDD: RDD[String] = actionRDD.filter {
      action => {
        val datas: Array[String] = action.split("_")
        datas(6) != "-1"
      }
    }
    val clickCountRDD: RDD[(String, Int)] = clickActionRDD.map {
      action => {
        val datas: Array[String] = action.split("_")
        (datas(6), 1)
      }
    }.reduceByKey(_ + _)

    //3.统计品类的下单数量 (品类ID,下单数量)
    val orderActionRDD: RDD[String] = actionRDD.filter {
      action => {
        val datas: Array[String] = action.split("_")
        datas(8) != "null"
      }
    }
    val orderCountRDD: RDD[(String, Int)] = orderActionRDD.flatMap {
      action => {
        val datas: Array[String] = action.split("_")
        val cids: Array[String] = datas(8).split(",")
        cids.map((_, 1))
      }
    }.reduceByKey(_ + _)

    //4.统计品类的支付数量 (品类ID,支付数量)
    val payActionRDD: RDD[String] = actionRDD.filter {
      action => {
        val datas: Array[String] = action.split("_")
        datas(10) != "null"
      }
    }
    val payCountRDD: RDD[(String, Int)] = payActionRDD.flatMap {
      action => {
        val datas: Array[String] = action.split("_")
        val cids: Array[String] = datas(10).split(",")
        cids.map((_, 1))
      }
    }.reduceByKey(_ + _)

    //问题2:cogroup()算子底层会走shuffle,最好换掉..
    //5.按照品类进行排序,取热门品类前10名
    //union方式实现满外连接，需要先补0
    // (品类ID,点击数量)  => (品类ID,(点击数量,0,0))
    // (品类ID,下单数量)  => (品类ID,(0,下单数量,0))
    // (品类ID,支付数量)  => (品类ID,(0,0,支付数量))
    // (品类ID,(点击数量,下单数量,支付数量))
    val clickRDD: RDD[(String, (Int, Int, Int))] = clickCountRDD.map {
      case (cid, cnt) => {
        (cid, (cnt, 0, 0))
      }
    }
    val ordrRDD: RDD[(String, (Int, Int, Int))] = orderCountRDD.map {
      case (cid, cnt) => {
        (cid, (0, cnt, 0))
      }
    }
    val payRDD: RDD[(String, (Int, Int, Int))] = payCountRDD.map {
      case (cid, cnt) => {
        (cid, (0, 0, cnt))
      }
    }
    //union的方式实现满外连接
    val unionRDD: RDD[(String, (Int, Int, Int))] = clickRDD.union(ordrRDD).union(payRDD)

    val unionRDD2: RDD[(String, (Int, Int, Int))] = unionRDD.reduceByKey {
      (t1, t2) => {
        (t1._1 + t2._1, t1._2 + t2._2, t1._3 + t2._3)
      }
    }

    //倒序排序取前10
    val result: Array[(String, (Int, Int, Int))] = unionRDD2.sortBy(_._2, false).take(10)

    //6.将结果采集到控制到打印输出
    result.foreach(println)

    //TODO 3 关闭资源
    sc.stop()
  }
}
