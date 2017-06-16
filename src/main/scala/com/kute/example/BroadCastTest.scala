package com.kute.example

import org.apache.spark.sql.SparkSession

/**
 * Created by longbai on 2017/6/16.
 */
object BroadCastTest {

  def main(args: Array[String]) {

    val spark = SparkSession.builder().appName("broadcast app")
    .master("local[*]").getOrCreate()

    val data = (1 to 10).toList

    for(i <- 1 until 3) {

      println("iter " + i)
      val broadcastData = spark.sparkContext.broadcast(data)

      val df = spark.sparkContext.parallelize(1 to 20).map(t => broadcastData.value.size)

      df.collect().foreach(println)

    }

    spark.stop()

  }

}
