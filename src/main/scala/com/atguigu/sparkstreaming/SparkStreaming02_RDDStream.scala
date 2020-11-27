package com.atguigu.sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

object SparkStreaming02_RDDStream {
  def main(args: Array[String]): Unit = {
    val ssc = new StreamingContext(new SparkConf().setMaster("local[*]").setAppName("sparkstreaming"),Seconds(4))
    val rddQueue = new mutable.Queue[RDD[Int]]()
    val inputDStream: InputDStream[Int] = ssc.queueStream(rddQueue, oneAtATime = false)

    val sumDStream: DStream[Int] = inputDStream.reduce(_+_)

    sumDStream.print()

    ssc.start()

    for (i <- 1 to 5) {
      rddQueue += ssc.sparkContext.makeRDD(1 to 5)
      Thread.sleep(2000)


      ssc.awaitTermination()
    }

  }
}
