package com.atguigu.sparkstreaming

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver


/**
 *
 * @param host : 主机名称
 * @param port ： 端口号
 *  Receiver[String] ：返回值类型：String
 *  StorageLevel.MEMORY_ONLY： 返回值存储方式
 */
class CustomerReceiver (host:String, port: Int) extends Receiver[String](StorageLevel.MEMORY_ONLY){

  // 最初启动的时候，调用该方法，作用为：读数据并将数据发送给Spark
  override def onStart(): Unit = {
    new Thread("Socket Receiver"){
      override def run(): Unit ={
        receive()
      }
    }.start()
  }
  def receive():Unit={

  }


  override def onStop(): Unit = ???
}
