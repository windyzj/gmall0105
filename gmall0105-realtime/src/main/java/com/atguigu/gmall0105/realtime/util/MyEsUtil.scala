package com.atguigu.gmall0105.realtime.util

import java.util
import java.util.Objects

import io.searchbox.client.{JestClient, JestClientFactory}
import io.searchbox.client.config.HttpClientConfig
import io.searchbox.core.{Bulk, BulkResult, Index}

object MyEsUtil {
  private val ES_HOST = "http://hadoop1"
  private val ES_HTTP_PORT = 9200
  private var factory:JestClientFactory = null

  /**
    * 获取客户端
    *
    * @return jestclient
    */
  def getClient: JestClient = {
    if (factory == null) build()
    factory.getObject
  }

  /**
    * 关闭客户端
    */
  def close(client: JestClient): Unit = {
    if (!Objects.isNull(client)) try
      client.shutdownClient()
    catch {
      case e: Exception =>
        e.printStackTrace()
    }
  }

  /**
    * 建立连接
    */
  private def build(): Unit = {
    factory = new JestClientFactory
    factory.setHttpClientConfig(new HttpClientConfig.Builder(ES_HOST + ":" + ES_HTTP_PORT).multiThreaded(true)
      .maxTotalConnection(20) //连接总数
      .connTimeout(10000).readTimeout(10000).build)

  }
 // 批量插入数据到ES
  def  insertBulk(indexName:String,docList:List[(String,Any)]): Unit ={
    if(docList.size>0){
      val jest: JestClient = getClient
      val bulkBuilder = new Bulk.Builder().defaultIndex(indexName).defaultType("_doc")
      for ((id,doc) <- docList ) {
        val indexBuilder = new Index.Builder(doc)
        if(id!=null){
          indexBuilder.id(id)
        }
        val index: Index = indexBuilder.build()
        bulkBuilder.addAction(index)
      }
      val bulk: Bulk = bulkBuilder.build()
      val items: util.List[BulkResult#BulkResultItem] = jest.execute( bulk).getItems
      println("保存"+items.size()+"条到ES!")
      close(jest)
    }
  }



  def main(args: Array[String]): Unit = {
    val jest: JestClient = getClient

     val index: Index = new Index.Builder(Stud("zhang3","zhang33")).index("gmall0105_stud").`type`("_doc").id("stu123").build()
    jest.execute(index)

  }


  case class Stud(name:String ,nickname:String){

  }



}
