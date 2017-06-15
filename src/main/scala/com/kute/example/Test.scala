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
//      访问控制：http://spark.apache.org/docs/latest/configuration.html#security
      .config("spark.acls.enable", true)
      .config("spark.admin.acls", "lilei,longbai") //* 表示任何人
      .appName("spark test")
      .master("local[*]")
      .getOrCreate()

    val list1: List[(String, Int)] = List("R1" -> 3, "R2" -> 5, "R3" -> 5, "R4" -> 5, "R5" -> 3)
    val list2: List[(String, Int)] = List("R2" -> 51, "R3" -> 51, "R4" -> 51, "R5" -> 31)
    val rdd1 = spark.sparkContext.makeRDD(list1)
    val rdd2 = spark.sparkContext.makeRDD(list2)

    rdd1.leftOuterJoin(rdd2).filter(t => t._2._2.isDefined).foreach(println)

    spark.close()
  }

}
