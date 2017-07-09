package com.kute.suite

/**
 * Created by kute on 2017/7/9.
 */
object SparkException {

  case object SparkContextAlreadyInitialized extends IllegalStateException

  case class SparkConfigAfterInitialization(confs: Seq[(String, String)])
    extends IllegalStateException(
      s"Attempting to configure SparkContext after initialization:\n" +
        (
          for {
            (k, v) ← confs
          } yield
          s"$k:\t$v"
          )
          .mkString("\t", "\n\t", "")
    )

  case object NoSparkContextToClear extends IllegalStateException

}
