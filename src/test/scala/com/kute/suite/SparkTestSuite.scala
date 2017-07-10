package com.kute.suite

import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkContext
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.streaming.StreamingContext
import org.hammerlab.test.Suite

/**
 * Created by kute on 2017/7/9.
 */
class SparkTestSuite extends Suite with SparkTestSuiteBase{

  protected implicit var session: SparkSession = _
  protected implicit var sqlc: SQLContext = _
  protected implicit var sc: SparkContext = _
  protected implicit var ssc: StreamingContext = _
  protected implicit var hadoopConf: Configuration = _

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    session = getOrCreateSparkSession()
    sc = session.sparkContext
    sc.setCheckpointDir(checkPointDir.toString())
    ssc = makeStreamingContext
    sqlc = session.sqlContext
    hadoopConf = sc.hadoopConfiguration
  }

  override def afterAll(): Unit = {
    super.afterAll()
    session.stop()
    session = null
    sc = null
    sqlc = null
    hadoopConf = null
  }

}
