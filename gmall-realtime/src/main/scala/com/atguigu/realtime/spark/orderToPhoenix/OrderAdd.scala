package com.atguigu.realtime.spark.orderToPhoenix

import com.alibaba.fastjson.JSON
import com.atguigu.gmall.common.constant.GmallConstants
import com.atguigu.realtime.spark.bean.OrderBean
import com.atguigu.realtime.spark.util.MykafkaUtil
import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.phoenix.spark._

object OrderAdd {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("order_in").setMaster("local[*]")
    val ssc = new StreamingContext(conf,Seconds(3))
    val DStream = MykafkaUtil.getKafkaStream(GmallConstants.TOPIC_ORDER,ssc)

    val mappedDStream = DStream.map(ele => {
      val json = ele.value()
      val orderBean = JSON.parseObject(json, classOf[OrderBean])
      val strings = orderBean.create_time.split(" ")
      orderBean.create_date = strings(0)
      orderBean.create_hour = strings(1).split(":")(0)
     val tuple = orderBean.consignee_tel.splitAt(4)
      orderBean.consignee_tel =tuple._1+"*******"
        orderBean

    })

//    mappedDStream.foreachRDD(ele=>{
//      println(ele.collect().mkString("\n"))
//    })
    mappedDStream.foreachRDD(ele=>{
      ele.saveToPhoenix("GMALL2019_ORDER_INFO",Seq("ID","PROVINCE_ID", "CONSIGNEE", "ORDER_COMMENT", "CONSIGNEE_TEL", "ORDER_STATUS", "PAYMENT_WAY", "USER_ID","IMG_URL", "TOTAL_AMOUNT", "EXPIRE_TIME", "DELIVERY_ADDRESS", "CREATE_TIME","OPERATE_TIME","TRACKING_NO","PARENT_ORDER_ID","OUT_TRADE_NO", "TRADE_BODY", "CREATE_DATE", "CREATE_HOUR"),new Configuration(),Some("hadoop102,hadoop103,hadoop104:2181"))
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
