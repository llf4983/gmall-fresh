package com.atguigu.realtime.spark.alert

import java.util

import com.alibaba.fastjson.JSON
import com.atguigu.gmall.common.constant.GmallConstants
import com.atguigu.realtime.spark.bean.{AlertInfo, EventInfo}
import com.atguigu.realtime.spark.util.{ESUtil, MykafkaUtil}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.util.control.Breaks

object AlertApplication {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("alert").setMaster("local[*]")
    val ssc = new StreamingContext(conf,Seconds(3))
    val kafkaDStream = MykafkaUtil.getKafkaStream(GmallConstants.TOPIC_EVENT,ssc)
    val eventDStream = kafkaDStream.map(ele => {
      val json = ele.value()
      val info = JSON.parseObject(json, classOf[EventInfo])
      info
    })
    val windowDStream = eventDStream.window(Seconds(300),Seconds(3))

    val groupDStream = windowDStream.map(ele=>(ele.mid,ele)).groupByKey()
    val booleanDStream = groupDStream.map(ele => {
      val coupon = new util.HashSet[String]()
      val itemId = new util.HashSet[String]()
      val event = new util.ArrayList[String]()
      var click = false
      Breaks.breakable(
        for (elem <- ele._2) {
          event.add(elem.evid)
          if ("coupon".equals(elem.evid)) {
            coupon.add(elem.uid)
            itemId.add(elem.itemid)
          }
          if ("clickItem".equals(elem.evid)) {
            click = true
            Breaks.break()
          }
        }
      )

      (coupon.size() >= 3 && !click, AlertInfo(ele._1, coupon, itemId, event, System.currentTimeMillis()))
    })
//    booleanDStream.foreachRDD(ele=>{
//      println(ele.collect().mkString("\n"))
//    })
    val mapDS = booleanDStream.filter(_._1).map(_._2)
    mapDS.foreachRDD(ele=>{
      ele.foreachPartition(ele=>{
        val list = ele.toList
        val tuples = list.map(el => {
          (el.mid + "_" + el.ts / 1000 / 60, el)
        })
        ESUtil.putData(GmallConstants.ES_ALETR_INDEX,tuples)
      })
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
