package com.kute.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Created by kute on 2017/6/12.
 */
object StreamingContextCreate {

  val checkPoint = "checkpoint"

  private def createFunction(): StreamingContext ={
    val conf = new SparkConf().setMaster("local[*]").setAppName("wordCountApp")
      .set("spark.ui.port", "4041")
      .set("spark.executor.heartbeatInterval", "3500s")
      .set("spark.rpc.askTimeout", "600s")

    val ssc = new StreamingContext(conf, Seconds(10))
    ssc.checkpoint(checkPoint)
    println("==============================create new context============================")
    ssc
  }

  val streamingContext = StreamingContext.getOrCreate(checkPoint, createFunction)

}
