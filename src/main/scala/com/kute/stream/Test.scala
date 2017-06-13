package com.kute.stream

import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by longbai on 2017/5/22.
 */
class Test {

}

object Test {
  def main (args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("app").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val data = 1 to 100 toList

    val rdd = sc.parallelize(data)

    val count = rdd.reduce(_ + _)

    println("============================")
    println(count)
    println("============================")

    sc.stop()

  }
}
