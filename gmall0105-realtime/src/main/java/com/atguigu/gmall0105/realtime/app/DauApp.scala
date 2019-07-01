package com.atguigu.gmall0105.realtime.app

import java.{lang, util}
import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.JSON
import com.atguigu.gmall.constant.GmallConstants
import com.atguigu.gmall0105.realtime.bean.StartupLog
import com.atguigu.gmall0105.realtime.util.{MyKafkaUtil, RedisUtil}
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis
import org.apache.phoenix.spark._

object DauApp {

  def main(args: Array[String]): Unit = {
     val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("dau_app")
     val ssc = new StreamingContext(sparkConf,Seconds(5))

    val inputDstream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_STARTUP,ssc)

//    inputDstream.foreachRDD{rdd=>
//      println(rdd.map(_.value()).collect().mkString("\n"))
//
//    }
    //0 数据流 转换 结构变成case class 补充两个时间字段
    val startuplogDstream: DStream[StartupLog] = inputDstream.map { record =>
      val jsonStr: String = record.value()
      val startupLog: StartupLog = JSON.parseObject(jsonStr, classOf[StartupLog])

      val dateTimeStr: String = new SimpleDateFormat("yyyy-MM-dd HH").format(new Date(startupLog.ts))
      val dateArr: Array[String] = dateTimeStr.split(" ")
      startupLog.logDate = dateArr(0)
      startupLog.logHour = dateArr(1)
      startupLog
    }


    startuplogDstream.cache()


   // 1   利用用户清单进行过滤 去重  只保留清单中不存在的用户访问记录

// 反复查询redis性能有消耗
//    startuplogDstream.filter { startuplog =>
//      val jedis: Jedis = RedisUtil.getJedisClient //executor
//      val key = "dau:" + startuplog.logDate
//      val flag: lang.Boolean = jedis.sismember(key, startuplog.mid)
//      jedis.close()
//      !flag
//    }

    // 清单只查询一次 不会更新
//    val jedis: Jedis = RedisUtil.getJedisClient //driver 只执行一次
//    val dateStr: String = new SimpleDateFormat("yyyy-MM-dd").format(new Date())
//
//    val key = "dau:" +  dateStr
//    val dauMidSet: util.Set[String] = jedis.smembers(key)
//
//    val dauMidBC: Broadcast[util.Set[String]] = ssc.sparkContext.broadcast(dauMidSet)
//
//    startuplogDstream.filter{ startuplog=> //executor
//      val dauMidSet: util.Set[String] = dauMidBC.value
//      !dauMidSet.contains(startuplog)
//    }

    val filteredDstream: DStream[StartupLog] = startuplogDstream.transform { rdd =>
      val jedis: Jedis = RedisUtil.getJedisClient //driver //按周期执行
    val dateStr: String = new SimpleDateFormat("yyyy-MM-dd").format(new Date())

      val key = "dau:" + dateStr
      val dauMidSet: util.Set[String] = jedis.smembers(key)
      jedis.close()

      val dauMidBC: Broadcast[util.Set[String]] = ssc.sparkContext.broadcast(dauMidSet)
      println("过滤前：" + rdd.count())
      val filteredRDD: RDD[StartupLog] = rdd.filter { startuplog => //executor
        val dauMidSet: util.Set[String] = dauMidBC.value
        !dauMidSet.contains(startuplog.mid)
      }
      println("过滤后：" + filteredRDD.count())
      filteredRDD

    }

    //  批次内进行去重：：按照mid 进行分组，每组取第一个值
    val groupbyMidDstream: DStream[(String, Iterable[StartupLog])] = filteredDstream.map(startuplog=>(startuplog.mid,startuplog)).groupByKey()
    val distictDstream: DStream[StartupLog] = groupbyMidDstream.flatMap { case (mid, startupLogItr) =>
      startupLogItr.toList.take(1)
    }







   // 2 保存今日访问过的用户(mid)清单   -->Redis    1 key类型 ： set    2 key ： dau:2019-xx-xx   3 value : mid
    distictDstream.foreachRDD{rdd=>
      //driver
      rdd.foreachPartition{ startuplogItr=>
        val jedis:Jedis=RedisUtil.getJedisClient   //executor
        for (startuplog <- startuplogItr ) {
                  val key= "dau:"+startuplog.logDate
                  jedis.sadd(key,startuplog.mid)
                  println(startuplog)
        }
        jedis.close()
      }
//      rdd.foreach{ startuplog=>    //executor
//        val key= "dau:"+startuplog.logDate
//        jedis.sadd(key,startuplog.mid)
//
//
//
//      }

    }

    //把数据写入hbase+phoenix
    distictDstream.foreachRDD{rdd=>
      rdd.saveToPhoenix("GMALL0105_DAU",Seq("MID", "UID", "APPID", "AREA", "OS", "CH", "TYPE", "VS", "LOGDATE", "LOGHOUR", "TS") ,new Configuration,Some("hadoop1,hadoop2,hadoop3:2181"))
    }



    ssc.start()
    ssc.awaitTermination()

  }

}
