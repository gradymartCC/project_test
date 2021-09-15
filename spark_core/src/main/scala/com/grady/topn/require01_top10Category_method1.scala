package com.grady.topn

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object require01_top10Category_method1 {
  def main(args: Array[String]): Unit = {
    //TODO 1 创建SparkConf配置文件,并设置App名称
    val conf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")
    //TODO 2 利用SparkConf创建sc对象
    val sc = new SparkContext(conf)
    //1.读取原始日志数据
    val actionRDD: RDD[String] = sc.textFile("input\\user_visit_action.txt")
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

    //5.按照品类进行排序,取热门品类前10名
    //热门排名:先按照点击排,然后按照下单数量排,最后按照支付数量排
    //元组排序:先比较第一个,再比较第二个,最后比较第三个
    //(品类ID,(点击数量,下单数量,支付数量))
    //三个RDD需要进行满外连接,因此需要用到cogroup()算子
    val cogroupRDD: RDD[(String, (Iterable[Int], Iterable[Int], Iterable[Int]))] = clickCountRDD.cogroup(orderCountRDD, payCountRDD)

    val cogroupRDD2: RDD[(String, (Int, Int, Int))] = cogroupRDD.mapValues {
      case (iter1, iter2, iter3) => {
        var clickCnt =0
        val clickIter: Iterator[Int] = iter1.iterator
        if (clickIter.hasNext) {
          clickCnt = clickIter.next()
        }

        var orderCnt =0
        val orderIter: Iterator[Int] = iter2.iterator
        if (orderIter.hasNext) {
          orderCnt = orderIter.next()
        }

        var payCnt =0
        val payIter: Iterator[Int] = iter3.iterator
        if (payIter.hasNext) {
          payCnt = payIter.next()
        }

        (clickCnt,orderCnt,payCnt)
      }
    }
    //倒序排序取前10
    val result: Array[(String, (Int, Int, Int))] = cogroupRDD2.sortBy(_._2, false).take(10)

    //6.将结果采集到控制到打印输出
    result.foreach(println)

    //TODO 3 关闭资源
    sc.stop()
  }
}