package com.kute.example

/**
 * Created by kute on 2017/5/24.
 */

import scala.collection.JavaConverters._
import org.apache.spark.util.Utils

object Test {

  def main(args: Array[String]) {

    val env = System.getenv()

    // java collection to scala collection
    env.asScala.foreach(print)



  }

}
