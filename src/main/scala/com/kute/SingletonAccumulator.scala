package com.kute

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.util.LongAccumulator

/**
 * Created by longbai on 2017/6/16.
 */
object SingletonAccumulator {

  @volatile var longInstance: LongAccumulator = _

  @volatile var castInstance: Broadcast[Long] = _

  def getLongInstance(sparkContext: SparkContext): LongAccumulator = {
    if(null == longInstance) {
      synchronized {
        if(null == longInstance) {
          longInstance = sparkContext.longAccumulator
        }
      }
    }
    longInstance
  }

  def getCastInstance(sparkContext: SparkContext): Broadcast[Long] = {
    if(null == castInstance) {
      synchronized {
        if(null == castInstance) {
          castInstance = sparkContext.broadcast(0)
        }
      }
    }
    castInstance
  }

}
