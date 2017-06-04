package com.kute.example

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by kute on 2017/5/20.
 */

class BasicOperation {

}

object BasicOperation {

  def main (args: Array[String]){

    val file = "/Users/kute/work/dataset/postscndryunivsrvy2013dirinfo.csv"

//    val conf = new SparkConf().setAppName("app").setMaster("spark://kutembp:7077")
//    val sc = new SparkContext(conf)

    val session = SparkSession.builder().appName("app2").master("spark://kutembp:7077").getOrCreate()
    val sc = session.sparkContext

    try {
      val text = sc.textFile(file).cache()

      val count = text.flatMap(_.split(",")).map((_, 1)).reduceByKey((sum, num) => sum + num)

      println("=====================================")
      println(count.take(10).foreach(println))
      println("=====================================")

      val df = session.read.option("header","true").csv(file)
      df.take(10).foreach(println)

    } finally {
      sc.stop()
    }

  }

}
