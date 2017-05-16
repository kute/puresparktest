package com.kute.stream

import org.apache.spark._
import org.apache.spark.streaming._

/**
 * Created by kute on 16/4/13.
 */

object WindowOperation {

  def main (args: Array[String]){

    val conf = new SparkConf().setMaster("local[2]").setAppName("wordCountApp")

    val ssc = new StreamingContext(conf, Seconds(1))

    val lines = ssc.socketTextStream("localhost", 1572)

    val workds = lines.flatMap(_.split(" ")).map((_, 1))

//    workds.reduceByKey((a, b) => a + b).print()

    println()

    workds.reduceByKeyAndWindow((a: Int, b: Int) => a + b, Seconds(30), Seconds(10)).print()

    ssc.start()
    ssc.awaitTermination()

  }
}
