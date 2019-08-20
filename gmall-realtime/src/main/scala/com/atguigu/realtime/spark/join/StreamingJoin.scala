package com.atguigu.realtime.spark.join

import com.alibaba.fastjson.JSON
import com.atguigu.gmall.common.constant.GmallConstants
import com.atguigu.realtime.spark.bean.{JoinedTableBean, OrderBean, OrderDetail, UserInfo}
import com.atguigu.realtime.spark.util.{ESUtil, MykafkaUtil, RedisUtil}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.json4s.native.Serialization

import scala.collection.mutable.ListBuffer

object StreamingJoin {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("join").setMaster("local[*]")
    val ssc = new StreamingContext(conf,Seconds(3))

    val orderJSONDStream = MykafkaUtil.getKafkaStream(GmallConstants.TOPIC_ORDER,ssc)
    val orderDetailJSONDStream = MykafkaUtil.getKafkaStream(GmallConstants.TOPIC_ORDER_DETAIL,ssc)
    val userJSONDStream = MykafkaUtil.getKafkaStream(GmallConstants.TOPIC_USER,ssc)

    val mappedOrderDStream = orderJSONDStream.map(ele=>{
      val bean = JSON.parseObject(ele.value(),classOf[OrderBean])
      val strings = bean.create_time.split(" ")
      bean.create_date=strings(0)
      bean.create_hour=strings(1).split(":")(0)
      bean.consignee_tel=bean.consignee_tel.splitAt(4)._1+"*******"
      (bean.id,bean)
    })
    val mappedDetailDStream = orderDetailJSONDStream.map(ele=>{
      val bean = JSON.parseObject(ele.value,classOf[OrderDetail])
      (bean.order_id,bean)
    })

    val fullJoinDStream = mappedOrderDStream.fullOuterJoin(mappedDetailDStream)
    val WideTableDStream = fullJoinDStream.flatMap(ele => {

      val beans = ListBuffer[JoinedTableBean]()
      val jedis = RedisUtil.getJedisClient
      if (ele._2._1 != None) {
        val orderObj = ele._2._1.get

        if (ele._2._2 != None) {
          val detailObj = ele._2._2.get
          beans += new JoinedTableBean(orderObj, detailObj)

        }
        val key = "orderInfo:" + orderObj.id
        implicit val formats=org.json4s.DefaultFormats
        jedis.setex(key, 3600, Serialization.write(orderObj))

        val strings = jedis.smembers("detail:" + orderObj.id)
        import scala.collection.JavaConversions._
        for (ele <- strings) {
          val detail = JSON.parseObject(ele, classOf[OrderDetail])
          beans += new JoinedTableBean(orderObj, detail)
        }
      } else {
        if (ele._2._2 != None) {
          val detailObj = ele._2._2.get
          val key = "detail:"+detailObj.order_id
          implicit val formats=org.json4s.DefaultFormats
          val detailJSON = Serialization.write(detailObj)
          jedis.sadd(key,detailJSON)
          jedis.expire(key,3600)
          val strings = jedis.get("orderInfo:" + detailObj.order_id)
//          import scala.collection.JavaConversions._
//          for (ele <- strings) {
//            val orderObj = JSON.parseObject(ele, classOf[OrderBean])
//            beans += new JoinedTableBean(orderObj, detailObj)
//          }
          if(strings!=null&&detailJSON.size>0){
            val bean = JSON.parseObject(strings,classOf[OrderBean])
            beans+=new JoinedTableBean(bean,detailObj)
          }
        }
      }
      jedis.close()
      beans
    })
    val resultDStream = WideTableDStream.mapPartitions(ele => {
      val jedis = RedisUtil.getJedisClient
      val beans = ListBuffer[JoinedTableBean]()
      for (elem <- ele) {
        val key = "user:" + elem.user_id
        val strings = jedis.get(key)
        if (strings != null) {
          val userObj = JSON.parseObject(strings, classOf[UserInfo])
          elem.combineUser(userObj)
          beans += elem
        }
      }

      jedis.close()
      beans.toIterator
    })

//    val resultDStream = WideTableDStream.mapPartitions(ele => {
//      val jedis = RedisUtil.getJedisClient
//      val beans = ele.map(elem => {
//        val key = "user:" + elem.user_id
//        val str = jedis.get(key)
//        if (str != null) {
//          val userInfoObj = JSON.parseObject(str, classOf[UserInfo])
//          elem.combineUser(userInfoObj)
//        }
//        elem
//      })
//      jedis.close()
//      beans
//    })
    resultDStream.foreachRDD(ele=>{
      ele.foreachPartition(elem=>{
        val list = elem.map(eleme => {
          (eleme.order_detail_id, eleme)
        }).toList
        ESUtil.putData(GmallConstants.ES_DETAIL_INDEX,list)
      })
    })
//    resultDStream.foreachRDD(ele=>{
//      println(ele.collect().mkString("\n"))
//    })

    val mappdeUserDStream = userJSONDStream.map(ele=>JSON.parseObject(ele.value,classOf[UserInfo]))
    mappdeUserDStream.foreachRDD(ele=>{
      ele.foreachPartition(elem=>{
        val jedis = RedisUtil.getJedisClient
        implicit val formats=org.json4s.DefaultFormats
        elem.foreach(eleme=>{
          val key = "user:"+eleme.id
          val userJSON = Serialization.write(eleme)
          jedis.set(key,userJSON)

        })

        jedis.close()
      })
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
