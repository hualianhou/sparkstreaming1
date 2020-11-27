package com.atguigu.project.app
import java.util.{Date, Properties}

import com.atguigu.project.handler.BlackListHandler
import com.atguigu.project.util.{MyKafkaUtil, PropertiesUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object RealTimeApp {

  def main(args: Array[String]): Unit = {

    //1.创建SparkConf
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("streaming")

    //2.创建StreamingContext
    val ssc = new StreamingContext(conf,Seconds(20))

    //3.读取数据、并转换为样例类对象；
    val kafka_topic = PropertiesUtil.load("config.properties").getProperty("kafka.topic")

    val adsLogDStream: DStream[Ads_log] = MyKafkaUtil.getKafkaStream(kafka_topic, ssc) // 拿到数据
      .map {                                                      //4.将从Kafka读出的数据转换为样例类对象
        kafkaData => {
          val arr: Array[String] = kafkaData.value().split(" ")
          Ads_log(arr(0).toLong, arr(1), arr(2), arr(3), arr(4))
        }
      }

    //5.需求一：根据MySQL中的黑名单过滤当前数据集
    val filtedAdsLogDStream: DStream[Ads_log] = BlackListHandler.filterByBlackList(adsLogDStream)

    //6.需求一：将满足要求的用户写入黑名单
    BlackListHandler.addBlackList(filtedAdsLogDStream)

    //测试打印
    filtedAdsLogDStream.cache()
    filtedAdsLogDStream.count().print()


    //启动任务
    ssc.start()
    ssc.awaitTermination()
  }
}

// 时间 地区 城市 用户id 广告id
case class Ads_log(timestamp: Long, area: String, city: String, userid: String, adid: String)
