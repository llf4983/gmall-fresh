package com.atguigu.realtime.spark.dau

import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.JSON
import com.atguigu.gmall.common.constant.GmallConstants
import com.atguigu.realtime.spark.bean.Startuplog
import com.atguigu.realtime.spark.util.{MykafkaUtil, RedisUtil}
import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.phoenix.spark._

object RealtimeStartupApp {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("gmall").setMaster("local[*]")
    val ssc = new StreamingContext(conf,Seconds(5))
    val DStream = MykafkaUtil.getKafkaStream(GmallConstants.TOPIC_START,ssc)

//    DStream.map(_.value()).foreachRDD(rdd=>{
//      println(rdd.collect().mkString("\n"))
//    })


    val classDStream = DStream.map(ele => {
      val str = ele.value()
      val startuplog = JSON.parseObject(str, classOf[Startuplog])
      val timeStamp = new SimpleDateFormat("yyyy-MM-dd HH").format(new Date(startuplog.ts))
      val timeStamps = timeStamp.split(" ")
      startuplog.logDate = timeStamps(0)
      startuplog.logHour = timeStamps(1)
      startuplog
    })

    classDStream.cache()

    val filterDStream = classDStream.transform(rdd => {
      val dates = new SimpleDateFormat("yyyy-MM-dd").format(new Date())
      val key = "dau:" + dates
      val jedis = RedisUtil.getJedisClient
      val strings = jedis.smembers(key)
      jedis.close()
      val broadcast = ssc.sparkContext.broadcast(strings)
      println("过滤前：" + rdd.count())
      val fil = rdd.filter(startLog => {
        !broadcast.value.contains(startLog.mid)
      })
      println("过滤后：" + fil.count())
      fil
    })

//    filterDStream.foreachRDD(rdd=>{
//      println(rdd.collect().mkString("\n"))
//    })
    val singleDStream = filterDStream.map(ele=>(ele.mid,ele)).groupByKey().flatMap(_._2.toList.take(1))

    singleDStream.foreachRDD(rdd=>{
      rdd.foreachPartition(ele=>{
        val jedis = RedisUtil.getJedisClient
        ele.foreach(ele=>{
          val key = "dau:"+ele.logDate
          jedis.sadd(key,ele.mid)
          println(ele)
        })
        jedis.close()
      })



    })

    singleDStream.foreachRDD{rdd=>
      rdd.saveToPhoenix("GMALL2019_DAU",Seq("MID", "UID", "APPID", "AREA", "OS", "CH", "TYPE", "VS", "LOGDATE", "LOGHOUR", "TS") ,new Configuration,Some("hadoop102,hadoop103,hadoop104:2181"))
    }

    ssc.start()
    ssc.awaitTermination()
  }
}
