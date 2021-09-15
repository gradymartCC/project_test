package com.grady.topn

import com.grady.topn.bean.{CategoryCountInfo, UserVisitAction}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object require01_top10Category_method2 {
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
    //3 转换数据结构 将actionRDD的数据变为 CategoryCountInfo(品类ID,1,0,0)
    val infoRDD: RDD[CategoryCountInfo] = actionRDD.flatMap(
      action => {
        if (action.click_category_id != "-1") {
          //点击的数据
          List(CategoryCountInfo(action.click_category_id, 1, 0, 0))
        } else if (action.order_category_ids != "null") {
          //下单的数据
          val arr: Array[String] = action.order_category_ids.split(",")
          arr.map(id => CategoryCountInfo(id, 0, 1, 0))
        } else if (action.pay_category_ids != "null") {
          //支付的数据
          val arr: Array[String] = action.pay_category_ids.split(",")
          arr.map(id => CategoryCountInfo(id, 0, 0, 1))
        } else {
          Nil
        }
      }
    )
    //4 按照品类id分组,将同一个品类的数据分到同一个组内
    val groupRDD: RDD[(String, Iterable[CategoryCountInfo])] = infoRDD.groupBy(_.categoryId)

    //5 将分组后的数据进行聚合处理 (品类id, (品类id, clickCount, OrderCount, PayCount))
    val reduceRDD: RDD[CategoryCountInfo] = groupRDD.mapValues(
      datas => {
        datas.reduce(
          (info1, info2) => {
            info1.orderCount += info2.orderCount
            info1.clickCount += info2.clickCount
            info1.payCount += info2.payCount
            info1
          }
        )
      }
    ).map(_._2)

    //6 将聚合后的数据 倒序排序 取前10
    val result: Array[CategoryCountInfo] = reduceRDD.sortBy(info => (info.clickCount, info.orderCount, info.payCount), false).take(10)
    result.foreach(println)

    //TODO 3 关闭资源
    sc.stop()
  }
}
