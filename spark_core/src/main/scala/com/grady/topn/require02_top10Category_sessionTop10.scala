package com.grady.topn

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import scala.collection.{immutable, mutable}

object require02_top10Category_sessionTop10 {
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
    //3 使用累加器统计相同品类id的点击数量,下单数量,支付数量
    //3.1 创建累加器
    val cateAcc = new CategoryCountAccumulator

    //3.2 注册累加器
    sc.register(cateAcc, "cateacc")

    //3.3 使用累加器
    actionRDD.foreach(action => cateAcc.add(action))

    //3.4 获得累加器的值 ((4,click),5961) ((4,order),1760) ((4,pay),1271)
    val accMap: mutable.Map[(String, String), Long] = cateAcc.value

    //4 将accMap按照品类id进行分组   (4,Map((4,click) -> 5961, (4,order) -> 1760, (4,pay) -> 1271))
    val groupMap: Map[String, mutable.Map[(String, String), Long]] = accMap.groupBy(_._1._1)

    //5 将groupMap转换成样例类集合
    val infoIter: immutable.Iterable[CategoryCountInfo] = groupMap.map {
      case (id, map) => {
        val click = map.getOrElse((id, "click"), 0L)
        val order = map.getOrElse((id, "order"), 0L)
        val pay = map.getOrElse((id, "pay"), 0L)
        CategoryCountInfo(id, click, order, pay)
      }
    }

    //6 对样例类集合倒序排序取前10
    val result: List[CategoryCountInfo] = infoIter.toList.sortBy(info => (info.clickCount, info.orderCount, info.payCount))(Ordering[(Long, Long, Long)].reverse).take(10)

    //********************需求二**********************
    //1 获取Top10热门品类
    val ids: List[String] = result.map(_.categoryId)
    //2 ids创建广播变量
    val bdIds: Broadcast[List[String]] = sc.broadcast(ids)
    //3 过滤原始数据,保留热门Top10的点击数据
    val filterActionRDD: RDD[UserVisitAction] = actionRDD.filter(
      action => {
        //一个会话一定会从点击开始,因此我们在这只要点击的数据
        if (action.click_category_id != "-1") {
          bdIds.value.contains(action.click_category_id)
        } else {
          false
        }
      }
    )
    //4 转换数据结构 UserVisitAction => (品类id-会话id,1)
    val idAndSessionToOneRDD: RDD[(String, Int)] = filterActionRDD.map(
      action => (action.click_category_id + "=" + action.session_id, 1)
    )

    //5 按照品类id-会话id分组聚合 (品类id-会话id,1)=>(品类id-会话id,sum)
    val idAndSessionToSumRDD: RDD[(String, Int)] = idAndSessionToOneRDD.reduceByKey(_ + _)

    //6 再次变化结构,分开品类和会话 (品类id-会话id,sum) => (品类id,(会话id,sum))
    val idToSessionAndSumRDD: RDD[(String, (String, Int))] = idAndSessionToSumRDD.map {
      case (key, sum) => {
        val keys: Array[String] = key.split("=")
        (keys(0), (keys(1), sum))
      }
    }

    //7 按照品类id分组 (品类id,(会话id,sum)) => (品类id,[(会话id,sum) ,(会话id,sum),....])
    val groupRDD: RDD[(String, Iterable[(String, Int)])] = idToSessionAndSumRDD.groupByKey()

    //8 对groupRDD的每个品类的集合倒序排序,求前10
    val resultRDD: RDD[(String, List[(String, Int)])] = groupRDD.mapValues(
      datas => {
        datas.toList.sortBy(_._2)(Ordering[Int].reverse)
      }.take(10)
    )

    resultRDD.collect().foreach(println)

    //TODO 3 关闭资源
    sc.stop()
  }
}
