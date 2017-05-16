package com.kute.stream

import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._

/**
 * Created by kute on 16/4/13.
 */

object QuickExample {

  def main (args: Array[String]){

    val conf = new SparkConf().setMaster("local[2]").setAppName("wordCountApp")

    val ssc = new StreamingContext(conf, Seconds(1))
    ssc.checkpoint(".")

    val lines = ssc.socketTextStream("localhost", 1572)

    val workds = lines.flatMap(_.split(" ")).map(word => (word, 1))

//    workds.reduceByKey((a, b) => a + b).print()


    val initRDD = ssc.sparkContext.parallelize(List("hello" -> 1, "world" -> 1))

    val updateFunc = (valus: Seq[Int], count: Option[Int]) => {
      println("==", valus.mkString(","), count, "==")
      Some(valus.sum + count.getOrElse(0))
    }

    val batchUpdateFunc = (iter: Iterator[(String, Seq[Int], Option[Int])]) => {
      iter.flatMap(t =>{ println(t._1 + "->" + t._2.mkString("$")); updateFunc(t._2, t._3).map(c => (t._1, c))})
    }

    workds.updateStateByKey(updateFunc)

    workds.updateStateByKey(updateFunc = batchUpdateFunc, new HashPartitioner(ssc.sparkContext.defaultParallelism), false)
    .print()

    ssc.start()
    ssc.awaitTermination()

  }
}
