package com.grady.topn.acc

import com.grady.topn.bean.UserVisitAction
import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable

//品类行为统计累加器
/**
 * 输入  UserVisitAction
 * 输出  ((鞋,click),1) ((鞋,order),1) ((鞋,pay),1)  mutable.Map[(String, String), Long]
 */
class CategoryCountAccumulator extends AccumulatorV2[UserVisitAction,mutable.Map[(String, String), Long]]{
  var map = mutable.Map[(String, String), Long]()

  override def isZero: Boolean = map.isEmpty

  override def copy(): AccumulatorV2[UserVisitAction, mutable.Map[(String, String), Long]] = new CategoryCountAccumulator

  override def reset(): Unit = map.clear()

  override def add(action: UserVisitAction): Unit = {
    if(action.click_category_id != "-1"){
      //点击  key=(cid,"click")
      val key = (action.click_category_id,"click")
      map(key) = map.getOrElse(key,0L) + 1L
    }else if(action.order_category_ids != "null"){
      //下单  key=(cid,"order")
      val cids: Array[String] = action.order_category_ids.split(",")
      for (cid <- cids) {
        val key = (cid,"order")
        map(key) = map.getOrElse(key,0L) + 1L
      }

    }else if(action.pay_category_ids != "null"){
      //支付  key=(cid,"pay")
      val cids: Array[String] = action.pay_category_ids.split(",")
      for (cid <- cids) {
        val key = (cid,"pay")
        map(key) = map.getOrElse(key,0L) + 1L
      }
    }
  }

  override def merge(other: AccumulatorV2[UserVisitAction, mutable.Map[(String, String), Long]]): Unit = {
    other.value.foreach {
      case (key, count) => {
        map(key) = map.getOrElse(key, 0L) + count
      }
    }
  }

  override def value: mutable.Map[(String, String), Long] = map
}
