package com.kute.streaming

import org.apache.spark._
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Created by kute on 16/4/13.
 * before running this, run 'nc -lk 1572' and input some data
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
    val lines = ssc.socketTextStream("localhost", 1572)
    //    val textDS = ssc.textFileStream("src/main/resources/docs")

    val words = lines.flatMap(_.split(" ")).map(word => (word, 1))

    // 统计当前的 bathInterval 内的词频统计
    words.reduceByKey(_ + _).print()


    val updateFunc = (valus: Seq[Int], count: Option[Int]) => {
      // count: 之前的时间 的次数
      // values: 当前数据
      //      println(valus.mkString(","), count)
      Some(valus.sum + count.getOrElse(0))
    }

    // 此为 updateStateByKey 的内部实现
    val batchUpdateFunc = (iter: Iterator[(String, Seq[Int], Option[Int])]) => {
      iter.flatMap(t =>{
        //t._1: 表示 key; t._2:表示 当前的batchInterval出现的频次的集合; t._3: 表示 之前的时间的 频次数
        //        println(t._1 + "->" + t._2.mkString("$") + "->" + t._3)
        updateFunc(t._2, t._3).map(c => (t._1, c))
      })
    }

    // 统计 所有时间 内的数据,根据state 更新
    words.updateStateByKey[Int](updateFunc).print()

//    val initRDD = ssc.sparkContext.parallelize(List("hello" -> 1, "world" -> 1))
//    val wordsCount = words.updateStateByKey(updateFunc = batchUpdateFunc,
//      new HashPartitioner(ssc.sparkContext.defaultParallelism), false, initRDD)

    //在DStream中进行 RDD的数据转换与清理: use transform
//    wordsCount.transform{rdd => {
//      rdd.filter(t => t._2 > 3)
//    }}

//    println("====foreachRDD======")
//    words.foreachRDD{rdd => {
//      rdd.foreachPartition{iter => {
//        //        val connection = ConnectionPool.getConnection();
//        iter.foreach {record => {
//          println(record)
//          //          record => connction.send(record)
//        }
//        }
//        //        ConnectionPool.releaseConnection(connection)
//      }}
//    }}

    //    println("=====foreachRDD with spark sql=====")
    //    //use spark sql
    //    words.foreachRDD(rdd => {
    //      val spark = SparkSession.builder().config(rdd.sparkContext.getConf).getOrCreate()
    //
    //      import spark.implicits._
    //
    //      val rddDF = rdd.toDF("word", "count")
    //
    //      rddDF.createOrReplaceTempView("table")
    //
    //      spark.sql("select word, sum(count) from table group by word").show(100)
    //    })

    //    wordsCount.print()
    ssc
  }

  val streamingContext = StreamingContext.getOrCreate(checkPoint, createFunction)

}

object QuickExample {

  def main (args: Array[String]){

    val ssc = StreamingContextCreate.streamingContext

    ssc.start()
    ssc.awaitTermination()
//    ssc.stop()

  }
}
