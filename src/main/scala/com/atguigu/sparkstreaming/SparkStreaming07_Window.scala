package com.atguigu.sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}


object SparkStreaming07_Window {
  def main(args: Array[String]): Unit = {
    val ssc = new StreamingContext(new SparkConf().setAppName("streaming").setMaster("local[*]"), Seconds(3))

   ssc.socketTextStream("localhost", 9999)
       .flatMap(_.split(" "))
       .map((_, 1))
       .window(Seconds(6), Seconds(3))  //窗口操作
       .reduceByKey(_ + _)
       .print()

    ssc.start()
    ssc.awaitTermination()
  }
}
