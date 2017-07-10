package com.kute.suite

import com.kute.suite.SparkException.{StreaingContextAlreadyInitialized, SparkConfigAfterInitialization}
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, Duration, StreamingContext}
import org.apache.spark.{SparkEnv, SparkConf, SparkContext}
import org.hammerlab.test.Suite

import scala.collection.mutable

/**
 * Created by kute on 2017/7/9.
 */
trait SparkTestSuiteBase extends Suite with LazyLogging{

  private var _session: SparkSession = _

  private var _ssc: StreamingContext = _

  private val _sparkConf = new SparkConf()

  def checkPointDir = tmpDir().toString()

  def numCores: Int = 4

  def batchDuration: Duration = Seconds(1)

  def getOrCreateSparkSession(clearDefaultSession: Boolean = false): SparkSession = Option(_session) match {
    case Some(_session) => {
      logger.info("SparkSession exists")
      _session
    }
    case None => {
      logger.error("SparkSession is not exists and will be created.")
      _session = SparkSession.builder().config(_sparkConf).getOrCreate()
      if(clearDefaultSession) {
        SparkSession.clearDefaultSession()
      }
      _session
    }
  }

  def makeStreamingContext(): StreamingContext = Option(_ssc) match {
    case Some(_) => {
      throw StreaingContextAlreadyInitialized
    }
    case None => {
      // create from exists sparkcontext
      _ssc = new StreamingContext(_session.sparkContext, batchDuration)
      _ssc.checkpoint(checkPointDir)
      _ssc
    }
  }

  def sparkConf(confs: (String, String)*): Unit =
    Option(_session) match {
      case Some(_session) =>
        throw SparkConfigAfterInitialization(confs)
      case None => {
        for ((k, v) <- confs) {
          _sparkConf.set(k, v)
        }
      }
    }

  sparkConf(
    // Set this explicitly so that we get deterministic behavior across test-machines with varying numbers of cores.
    "spark.master" → s"local[$numCores]",
    "spark.app.name" → this.getClass.getName,
    "spark.driver.host" → "localhost"
  )

}