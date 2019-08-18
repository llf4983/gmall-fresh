package com.atguigu.realtime.spark.util

import java.util.Objects

import io.searchbox.client.config.HttpClientConfig
import io.searchbox.client.{JestClient, JestClientFactory}
import io.searchbox.core.{Bulk, Index}

object ESUtil {
  private val esHost="http://hadoop102"
  private val esPort=9200
  private var factory:JestClientFactory=null

  def getClient():JestClient={
    if(factory==null){
      buildJestClient
    }
    factory.getObject
  }

  def buildJestClient():Unit={
    factory=new JestClientFactory
    val builder1 = new HttpClientConfig.Builder(esHost+":"+esPort)
    val builder2 = builder1.multiThreaded(true)
    val builder3 = builder2.maxTotalConnection(20)
    val builder4 = builder3.connTimeout(10000)
    val builder5 = builder4.readTimeout(10000)
    val config = builder5.build()
    factory.setHttpClientConfig(config)
  }

  def close(client: JestClient): Unit = {
    if (!Objects.isNull(client)) try
      client.shutdownClient()
    catch {
      case e: Exception =>
        e.printStackTrace()
    }
  }

  def putData(index:String,data:List[(String,Any)]): Unit ={
    if(data.size>0) {
      val client = getClient()
      val builder = new Bulk.Builder
      for (elem <- data) {
        val index1 = new Index.Builder(elem._2).index(index).`type`("_doc").id(elem._1).build()
        builder.addAction(index1)
      }
      val bulk = builder.build()
      client.execute(bulk)
      close(client)
    }
  }

  def main(args: Array[String]): Unit = {
    val client = getClient()
    val builder = new Index.Builder(Customer("zhangsan",1000.0))
    val builder1 = builder.index("customer")
    val builder2 = builder1.`type`("_doc")
    val builder3 = builder2.id("1")
    val index = builder3.build()

    client.execute(index)

    close(client)
  }
  case class Customer(
                     name:String,
                     amount:Double
                     )
}
