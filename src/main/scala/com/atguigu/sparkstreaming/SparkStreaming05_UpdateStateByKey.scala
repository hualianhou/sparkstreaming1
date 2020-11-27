package com.atguigu.sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}


object SparkStreaming05_UpdateStateByKey {
  def main(args: Array[String]): Unit = {
    val ssc = new StreamingContext(new SparkConf().setAppName("streaming").setMaster("local[*]"), Seconds(3))

    ssc.checkpoint("./ck")
    ssc.socketTextStream("hadoop102", 9999)
      .flatMap(_.split(" "))
      .map((_, 1))
//      .reduceByKey(_ + _)
      .updateStateByKey(
          // updateStateByKey会按照本次出现的单词逐个更新历史状态，本次没出现的单词保留不变，
          // 在整合某个单词的值时，newData表示当前批次中，这个单词的每次value的集合（该例中为（1，1，...），
          // oldState表示该单词历史汇总后出现的次数
          (newData: Seq[Int], oldState: Option[Int]) => {
            val newState = oldState.getOrElse(0) + newData.sum
            Option(newState)
          }
        ).print()


    


    ssc.start()
    ssc.awaitTermination()
  }}
