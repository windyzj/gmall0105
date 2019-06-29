package com.atguigu.gmall0105.realtime.app

import java.util

import com.alibaba.fastjson.JSON
import com.atguigu.gmall.constant.GmallConstants
import com.atguigu.gmall0105.realtime.bean.{CouponAlertInfo, EventInfo}
import com.atguigu.gmall0105.realtime.util.{MyEsUtil, MyKafkaUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.util.control.Breaks._

object EventApp {

  def main(args: Array[String]): Unit = {
      val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("event_app")

       val ssc = new StreamingContext(sparkConf,Seconds(5))

      val inputDstream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_EVENT,ssc)

      //1 格式转换成样例类
    val eventInfoDstream: DStream[EventInfo] = inputDstream.map { record =>
      val jsonstr: String = record.value()
      val eventInfo: EventInfo = JSON.parseObject(jsonstr, classOf[EventInfo])
      eventInfo
    }
    //2 开窗口
    val eventInfoWindowDstream: DStream[EventInfo] = eventInfoDstream.window(Seconds(30),Seconds(5))

    //3同一设备 分组
    val groupbyMidDstream: DStream[(String, Iterable[EventInfo])] = eventInfoWindowDstream.map(eventInfo=>(eventInfo.mid,eventInfo)).groupByKey()

    //4 判断预警
//    在一个设备之内
//    1 三次及以上的领取优惠券 (evid coupon) 且 uid都不相同
//    2 没有浏览商品(evid  clickItem)

    val checkCouponAlertDStream: DStream[(Boolean, CouponAlertInfo)] = groupbyMidDstream.map { case (mid, eventInfoItr) =>
      val couponUidsSet = new util.HashSet[String]()
      val itemIdsSet = new util.HashSet[String]()
      val eventIds = new util.ArrayList[String]()
      var notClickItem: Boolean = true
      breakable(
        for (eventInfo: EventInfo <- eventInfoItr) {
          eventIds.add(eventInfo.evid) //用户行为
          if (eventInfo.evid == "coupon") {
            couponUidsSet.add(eventInfo.uid) //用户领券的uid
            itemIdsSet.add(eventInfo.itemid) //用户领券的商品id
          } else if (eventInfo.evid == "clickItem") {
            notClickItem = false
            break()
          }
        }
      )
      //组合成元祖  （标识是否达到预警要求，预警信息对象）
      (couponUidsSet.size() >= 3 && notClickItem, CouponAlertInfo(mid, couponUidsSet, itemIdsSet, eventIds, System.currentTimeMillis()))
    }

    //过滤
    val filteredDstream: DStream[(Boolean, CouponAlertInfo)] = checkCouponAlertDStream.filter{_._1}


    //增加一个id 用于保存到es的时候进行去重操作
    val alertInfoWithIdDstream: DStream[(String, CouponAlertInfo)] = filteredDstream.map { case (flag, alertInfo) =>
      val period: Long = alertInfo.ts / 1000L / 60L
      val id: String = alertInfo.mid + "_" + period.toString
      (id, alertInfo)

    }


    alertInfoWithIdDstream.foreachRDD{rdd=>
      rdd.foreachPartition{alertInfoWithIdIter=>


        MyEsUtil.insertBulk( GmallConstants.ES_INDEX_COUPON_ALERT ,alertInfoWithIdIter.toList)

      }




    }



    ssc.start()
    ssc.awaitTermination()

  }



}
