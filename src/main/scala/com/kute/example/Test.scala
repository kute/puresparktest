package com.kute.example

/**
 * Created by kute on 2017/5/24.
 */

import org.apache.spark.sql.SparkSession

import scala.collection.JavaConverters._
import org.apache.spark.util.Utils

object Test {

  def main(args: Array[String]) {

//    val env = System.getenv()
    // java collection to scala collection
//    env.asScala.foreach(print)

    val spark = SparkSession.builder()
      .config("spark.executor.heartbeatInterval", "3500s")
      .config("spark.rpc.askTimeout", "600s")
      //avoid conflict with default port with spark-shell
      .config("spark.ui.port", 4042)
      .appName("spark test")
      .master("local[*]")
      .getOrCreate()

    val list1: List[(String, Int)] = List("R1" -> 3, "R2" -> 5, "R3" -> 5, "R4" -> 5, "R5" -> 3)
    val list2: List[((String, String), Double)] = List(("R1" -> "R3") -> 2, ("R2" -> "R3") -> 3, ("R2" -> "R5") -> 3, ("R1" -> "R2") -> 1)
    val rdd1 = spark.sparkContext.makeRDD(list1)
    val rdd2 = spark.sparkContext.makeRDD(list2)

    val map = rdd1.collectAsMap()

    rdd2.map(data => {
      data._1 -> 0.5 * (map.getOrElse(data._1._1, 0) + map.getOrElse(data._1._2, 0))
    })
  }

}
