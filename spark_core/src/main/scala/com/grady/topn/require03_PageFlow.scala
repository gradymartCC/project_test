package com.grady.topn

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object require03_PageFlow {
  def main(args: Array[String]): Unit = {
    //TODO 1 创建SparkConf配置文件,并设置App名称
    val conf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")
    //TODO 2 利用SparkConf创建sc对象
    val sc = new SparkContext(conf)
    //1 读取数据
    val lineRDD: RDD[String] = sc.textFile("input\\user_visit_action.txt")
    //2 封装样例类 将lineRDD变为actionRDD
    val actionRDD: RDD[UserVisitAction] = lineRDD.map(
      line => {
        val datas: Array[String] = line.split("_")
        //将解析出来的数据封装到样例类里面
        UserVisitAction(
          datas(0),
          datas(1),
          datas(2),
          datas(3),
          datas(4),
          datas(5),
          datas(6),
          datas(7),
          datas(8),
          datas(9),
          datas(10),
          datas(11),
          datas(12)
        )
      }
    )
    //3 准备过滤数据
    //定义需要统计的页面 (只统计集合中规定的页面跳转率) 分母过滤
    val ids = List("1", "2", "3", "4", "5", "6", "7")
    //定义对分子过滤的集合
    val zipIds: List[String] = ids.zip(ids.tail).map {
      case (p1, p2) => p1 + "-" + p2
    }

    //对ids创建广播变量
    val bdIds = sc.broadcast(ids)

    //4 计算分母 (页面,访问次数)
    val idsArr = actionRDD
      //过滤出要统计的page_id(由于最后一个页面总次数，不参与运算，所以也过滤了)
      .filter(action => bdIds.value.init.contains(action.page_id))
      .map(action => (action.page_id, 1))
      .reduceByKey(_ + _).collect()

    //array转成Map方便后续使用
    val idsMap: Map[String, Int] = idsArr.toMap

    //5 计算分子
    //5.1 按照session进行分组
    val sessionGroupRDD: RDD[(String, Iterable[UserVisitAction])] = actionRDD.groupBy(_.session_id)
    //5.2 将分组后的数据根据时间进行排序（升序）
    val page2pageRDD: RDD[(String, List[String])] = sessionGroupRDD.mapValues(
      datas => {
        //1 将迭代器转成list,然后按照行动时间升序排序
        val actions: List[UserVisitAction] = datas.toList.sortBy(_.action_time)
        //2 转变数据结构,获取到pageId
        val pageidList: List[String] = actions.map(_.page_id)
        //3 根据排好序的页面List拉链获得单跳元组 (1,2,3,4) zip (2,3,4)
        val pageToPageList: List[(String, String)] = pageidList.zip(pageidList.tail)
        //4 再次转变结构 元组变字符串 (1,2) => 1-2
        val pageJumpCounts: List[String] = pageToPageList.map {
          case (p1, p2) => p1 + "-" + p2
        }
        //5 对分子也进行过滤下,提升效率 只保留1-2,2-3,3-4,4-5,5-6,6-7的数据
        pageJumpCounts.filter(
          str => zipIds.contains(str)
        )
      }
    )

    val pageFlowRDD: RDD[List[String]] = page2pageRDD.map(_._2)

    //5.3 聚合统计结果
    val reduceFlowRDD: RDD[(String, Int)] = pageFlowRDD.flatMap(list => list).map((_, 1)).reduceByKey(_ + _)

    //6 分母和分组全部搞定,计算页面单跳转换率
    reduceFlowRDD.foreach{
      case (pageflow,sum) => {
        val pageIds: Array[String] = pageflow.split("-")

        //根据pageIds(0) 取分母的值
        val pageIdSum: Int = idsMap.getOrElse(pageIds(0), 1)
        //页面单跳转换率 = 分子/分母
        println(pageflow + " = " + sum.toDouble / pageIdSum)
      }
    }

    //TODO 3 关闭资源
    sc.stop()
  }
}
