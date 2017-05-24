package com.kute.example

import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by kute on 2017/5/20.
 */

class BasicOperation {

}

object BasicOperation {

  def main (args: Array[String]){

    val file = "/Users/kute/work/dataset/postscndryunivsrvy2013dirinfo.csv"

    val conf = new SparkConf().setAppName("app").setMaster("spark://kutembp:7077")

    val sc = new SparkContext(conf)

    val text = sc.textFile(file)

    val count = text.flatMap(s => s.split(",")).map((_, 1)).reduceByKey((sum, num) => sum + num)

    println("=====================================")
    println(count.take(100).foreach(println))
    println("=====================================")

    sc.stop()

  }

}
