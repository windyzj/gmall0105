package com.atguigu.gmall0105.realtime.app

import java.util

import com.alibaba.fastjson.JSON
import com.atguigu.gmall.constant.GmallConstants
import com.atguigu.gmall0105.realtime.bean.{OrderDetail, OrderInfo, SaleDetail}
import com.atguigu.gmall0105.realtime.util.{MyKafkaUtil, RedisUtil}
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.phoenix.spark._
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.json4s.native.Serialization
import redis.clients.jedis.Jedis

import scala.collection.mutable.ListBuffer
import collection.JavaConversions._

object SaleApp {

  def main(args: Array[String]): Unit = {
      val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sale_app")
      val ssc = new StreamingContext(sparkConf,Seconds(5))

     val inputDstream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_ORDER,ssc)
     val inputDetailDstream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_ORDER_DETAIL,ssc)
     val inputUserDstream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_USER,ssc)

    val orderInfoDstream: DStream[OrderInfo] = inputDstream.map { record =>
      val orderInfo: OrderInfo = JSON.parseObject(record.value(), classOf[OrderInfo])
     //补充日期字段
      val datetimeArr: Array[String] = orderInfo.create_time.split(" ")
      orderInfo.create_date = datetimeArr(0)
      val timeArr: Array[String] = datetimeArr(1).split(":")
      orderInfo.create_hour = timeArr(0)
      //电话脱敏
      val telTuple: (String, String) = orderInfo.consignee_tel.splitAt(7)
      orderInfo.consignee_tel = "*******" + telTuple._2

      orderInfo
    }

    val orderDetailDstream: DStream[OrderDetail] = inputDetailDstream.map { record =>
      val orderDetail: OrderDetail = JSON.parseObject(record.value(), classOf[OrderDetail])
      orderDetail
    }

    val orderInfoWithKeyDstream: DStream[(String, OrderInfo)] = orderInfoDstream.map(orderinfo=>(orderinfo.id,orderinfo))

    val orderDetailWithKeyDstream: DStream[(String, OrderDetail)] = orderDetailDstream.map(orderdetail=>(orderdetail.order_id,orderdetail))


     val fulljoinOrderDstream: DStream[(String, (Option[OrderInfo], Option[OrderDetail]))] = orderInfoWithKeyDstream.fullOuterJoin(orderDetailWithKeyDstream)


    val saleDetailDstream: DStream[SaleDetail] = fulljoinOrderDstream.mapPartitions { joinIter =>
      val jedis: Jedis = RedisUtil.getJedisClient
      implicit val formats=org.json4s.DefaultFormats
      val saleDetailList: ListBuffer[SaleDetail] = ListBuffer[SaleDetail]()
      for ((orderId, (orderInfoOption, orderDetailOption)) <- joinIter) {
        // 1 左右两边都有数据   说明 关联上了 =>把两个对象 拼接成一个新的大对象
        if (orderInfoOption != None && orderDetailOption != None) {
          val orderInfo: OrderInfo = orderInfoOption.get
          val orderDetail: OrderDetail = orderDetailOption.get
          println("关联上了:" + orderInfo.id)
          val saleDetail = new SaleDetail(orderInfo, orderDetail)
          saleDetailList += saleDetail

          //缓存主表， 即使主表被关联成功，但是 因为不能不保证之后的从表还需要关联该主表，所以主表必须缓存
          // redis   type string , key  order_info:[order_id]  ,value:: orderInfoJson
          val orderInfokey="order_info:"+orderInfo.id
          val orderInfoJson: String = Serialization.write(orderInfo)
          jedis.set(orderInfokey,orderInfoJson)


        }else if(orderInfoOption != None&&orderDetailOption == None){ //2  主表有 ，从表没有
          val orderInfo: OrderInfo = orderInfoOption.get
          println("主表有+++，从表没有---:" + orderInfo.id)
          //  1 来早了  2 来晚了
          //查询缓存判断来早还是来晚
          //查询从表  redis  type : set    key :  order_detail:[order_id] value: order_detail_json ++
          val orderDetailkey="order_detail:"+orderInfo.id
          val orderDetailJsonSet: util.Set[String] = jedis.smembers(orderDetailkey)
          if(orderDetailJsonSet!=null&&orderDetailJsonSet.size()>0){
            println("主表"+ orderInfo.id+"来晚了，领走:"+orderDetailJsonSet.size+"个从表对象" )
            for ( orderDetailJson <- orderDetailJsonSet ) {
                     val orderDetail: OrderDetail = JSON.parseObject(orderDetailJson,classOf[OrderDetail])
                     val saleDetail = new SaleDetail(orderInfo,orderDetail)
                      saleDetailList += saleDetail
            }
            jedis.del(orderDetailkey)

          }else{
            println("主表"+ orderInfo.id+"来早了"   )
          }
          //缓存主表， 即使主表被关联成功，但是 因为不能不保证之后的从表还需要关联该主表，所以主表必须缓存
          val orderInfokey="order_info:"+orderInfo.id
          val orderInfoJson: String = Serialization.write(orderInfo)
          jedis.set(orderInfokey,orderInfoJson)


        }else {  //3 主表没有，从表有
          val orderDetail: OrderDetail = orderDetailOption.get
          println("  主表没有----，从表有+++: order_id:" + orderDetail.order_id +"||order_detail_id:"+orderDetail.id)
          //从表判断来早还是来晚了  通过查询主表的缓存
          val orderInfokey="order_info:"+orderDetail.order_id
          val orderInfojson: String = jedis.get(orderInfokey)
          if(orderInfojson!=null&&orderInfojson.length>0){ //从表来晚
              println("从表来晚了 order_detail:"+orderDetail.id+" orderId:"+orderDetail.order_id)
              val orderInfo: OrderInfo = JSON.parseObject(orderInfojson,classOf[OrderInfo])
              val saleDetail = new SaleDetail(orderInfo,orderDetail)
              saleDetailList += saleDetail
          }else{  //从表来早了 把自己写入缓存
            //查询从表  redis  type : set    key :  order_detail:[order_id] value: order_detail_json ++
            println("从表来早了 order_detail:"+orderDetail.id+" orderId:"+orderDetail.order_id)
            val orderDetailKey="order_detail:"+orderDetail.order_id
              val orderDetailJson: String = Serialization.write(orderDetail)
              jedis.sadd(orderDetailKey,orderDetailJson)
              //jedis.expire(orderDetailKey,60)
          }


        }



      }


      jedis.close()
      saleDetailList.toIterator
    }
    saleDetailDstream.foreachRDD{rdd=>
      println(rdd.collect().mkString("\n"))
    }




      ssc.start()
      ssc.awaitTermination()

  }

}
