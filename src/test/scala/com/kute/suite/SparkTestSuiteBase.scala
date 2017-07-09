package com.kute.suite

import com.kute.suite.SparkException.SparkConfigAfterInitialization
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkEnv, SparkConf, SparkContext}
import org.hammerlab.test.Suite

import scala.collection.mutable

/**
 * Created by kute on 2017/7/9.
 */
trait SparkTestSuiteBase extends Suite with LazyLogging{

  private var _session: SparkSession = _

  private val sparkConfs = mutable.Map[String, String]()

  def checkPointDir = tmpDir()

  def numCores: Int = 4

  def getOrCreateSparkSession: SparkSession = Option(_session) match {
    case Some(_session) => {
      logger.info("SparkSession exists")
      _session
    }
    case None => {
      logger.info("SparkSession is not exists and will be created.")
      val sparkConf = new SparkConf()
      for {
        (k, v) <- sparkConfs
      } {
        sparkConf.set(k, v)
      }
      _session = SparkSession.builder().config(sparkConf).getOrCreate()
      _session
    }
  }

  def sparkConf(confs: (String, String)*): Unit =
    Option(_session) match {
      case Some(_session) ⇒
        throw SparkConfigAfterInitialization(confs)
      case None ⇒
        for {
          (k, v) ← confs
        } {
          sparkConfs(k) = v
        }
    }

  sparkConf(
    // Set this explicitly so that we get deterministic behavior across test-machines with varying numbers of cores.
    "spark.master" → s"local[$numCores]",
    "spark.app.name" → this.getClass.getName,
    "spark.driver.host" → "localhost"
  )

}