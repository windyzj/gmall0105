package com.atguigu.gmall0105.realtime.app

import com.alibaba.fastjson.JSON
import com.atguigu.gmall.constant.GmallConstants
import com.atguigu.gmall0105.realtime.bean.OrderInfo
import com.atguigu.gmall0105.realtime.util.MyKafkaUtil
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.phoenix.spark._
object OrderApp {

  def main(args: Array[String]): Unit = {
      val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("order_app")
      val ssc = new StreamingContext(sparkConf,Seconds(5))

       val inputDstream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_ORDER,ssc)

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

      // 增加一个字段 标识是否是用户首次下单
    }



    //保存到hbase
    orderInfoDstream.foreachRDD{rdd=>

      rdd.saveToPhoenix("gmall0105_order_info",Seq("ID","PROVINCE_ID", "CONSIGNEE", "ORDER_COMMENT", "CONSIGNEE_TEL", "ORDER_STATUS", "PAYMENT_WAY", "USER_ID","IMG_URL", "TOTAL_AMOUNT", "EXPIRE_TIME", "DELIVERY_ADDRESS", "CREATE_TIME","OPERATE_TIME","TRACKING_NO","PARENT_ORDER_ID","OUT_TRADE_NO", "TRADE_BODY", "CREATE_DATE", "CREATE_HOUR"),new Configuration,Some("hadoop1,hadoop2,hadoop3:2181") )

    }

    ssc.start()
    ssc.awaitTermination()

  }

}
