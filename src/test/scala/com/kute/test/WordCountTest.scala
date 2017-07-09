package com.kute.test

import com.kute.suite.SparkTestSuite
import org.hammerlab.spark.test.suite.SparkSuite

/**
 * Created by kute on 2017/7/8.
 */
class WordCountTest extends SparkTestSuite{

  override def numCores = 4

  sparkConf(
    "spark.master" -> s"local[$numCores]"
  )

  test("Word Count test") {
    val list1: List[(String, Int)] = List("R1" -> 3, "R2" -> 5, "R3" -> 5, "R4" -> 5, "R5" -> 3)
    val rdd1 = sc.makeRDD(list1)

    val result = rdd1.values.collect().reduce(_ + _)

    result should be (21)
  }

}
