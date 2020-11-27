package com.atguigu.sparkstreaming

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object rdd {
  def main(args: Array[String]): Unit = {
    val sparkstreamingConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkstreaming")
    val sc = new SparkContext(sparkstreamingConf)
      val unit: RDD[Int] = sc.makeRDD(List(1,2))

  }

}
