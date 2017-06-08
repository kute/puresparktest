package com.kute.sql

import akka.actor.Actor.Receive
import org.apache.spark.SparkConf
import akka.actor.{Props, ActorSystem, Actor, ActorLogging}
import org.apache.spark.sql.SparkSession

import scala.util.Random

/**
 * akka and spark sql
 * Created by kute on 2017/6/4.
 */

case class View(limit: Int)

class ActorMessage(conf: SparkConf) extends Actor with ActorLogging {

  var session: SparkSession = _

  override def preStart(): Unit = {
    session = SparkSession.builder().config(conf).getOrCreate()
    //avoid sharing same default session across multi actors
    SparkSession.clearDefaultSession()
  }

  override def postStop(): Unit = {
    if(session != null) {
      session.stop()
    }
  }

  val ids = 1 to 100
  val names = ('a' to 'z').map(_.toString)

  val beans = for {
    id <- ids
    name <- names
  } yield (id, name)

  def getbeans(limit: Int) = Random.shuffle(beans).take(limit)

  override def receive: Actor.Receive = {
    case View(limit) => {
      session.createDataFrame(getbeans(limit)).createOrReplaceTempView("test")
    }
    case sql: String => {
      log.info("receive sql:" + sql)
      session.sql(sql).show(100)
    }
    case _ => print("nothing")
  }

}

object ActorSqlTest {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("akka and spark sql").setMaster("local[*]")

    val system = ActorSystem("akka-and-spark-sql-system")

    val actors = (1 to 4).toList.map(n => system.actorOf(Props(new ActorMessage(conf)), "actor" + n))

    def getLimit = Random.nextInt(20)

    actors.foreach(_ ! View(getLimit))

    actors.foreach(_ ! "select * from test")

  }

}
