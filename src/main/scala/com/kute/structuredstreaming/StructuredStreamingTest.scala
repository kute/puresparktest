package com.kute.structuredstreaming

import com.kute.LogLevelUtil
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.slf4j.LoggerFactory

/**
 * Created by longbai on 2017/6/15.
 */

object StructuredStreamingTest {

  val checkPoint = "checkpoint"

  def main(args: Array[String]) {

    val conf = new SparkConf().setMaster("local[*]").setAppName("wordCountApp")
      .set("spark.ui.port", "4041")
      .set("spark.executor.heartbeatInterval", "3500s")
      .set("spark.rpc.askTimeout", "600s")
      .set("spark.worker.memory", "2g")

    val spark = SparkSession.builder().appName("spark structured streaming").config(conf).getOrCreate()

    import spark.implicits._

    // input table
    val linesDF = spark.readStream.format("socket")
    .option("host", "localhost")
    .option("port", 1572)
    .load()

    /* set up query */
    val wordsDS = linesDF
      .as[String] // DataFrame to DataSet
      .flatMap(_.split(" "))

    wordsDS.printSchema()
    // result table
    val wordsCountDF = wordsDS.groupBy("value").count()

    wordsCountDF.printSchema()

    /* set up query end here, and start begin with writestream.start() */
    val query = wordsCountDF.writeStream
      .outputMode("complete") // complete append update
      .format("console")
      .start()

    query.awaitTermination()

  }

}
