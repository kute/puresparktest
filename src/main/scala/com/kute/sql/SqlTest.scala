package com.kute.sql

import org.apache.spark.sql.{Encoders, SparkSession}

/**
 * Created by kute on 2017/6/3.
 */
class SqlTest {

}

case class Person(name: String, age: Long)

object SqlTest {
  def main(args: Array[String]) {

    val spark = SparkSession.builder()
      .config("spark.executor.heartbeatInterval", 350)
      .config("spark.worker.memory", "1g")
      .appName("spark sql app")
      .master("local")
      .getOrCreate()

//    basic(spark)

    interoperating(spark)


    spark.close()

  }

  def basic(spark: SparkSession): Unit = {
    import spark.implicits._

    val jsonfile = "src/main/resources/person.json"
    val df = spark.read.json(jsonfile)

    df.show()

    df.printSchema()

    df.select("name").show()

    df.select($"name", $"age" + 1).show()

    df.filter($"age" > 19).show()

    df.groupBy("name").count().show()

    //    session-scoped
    df.createOrReplaceTempView("personTable")

    spark.sql("select * from personTable").show()

    df.createGlobalTempView("personTb")
    //    Global temporary view is tied to a system preserved database `global_temp`
    spark.sql("select * from global_temp.personTb").show()

    //    Global temporary view is cross-session
    spark.newSession().sql("select * from global_temp.personTb").show()

    Seq(Person("kute", 19)).toDS().show()
    Seq(1, 2, 3).toDS().map(_ + 1).collect()

    spark.read.json(jsonfile).as[Person].show()
  }

  def interoperating(spark: SparkSession): Unit = {
    import spark.implicits._
    val df = spark.sparkContext
      .textFile("src/main/resources/person.text")
      .map(_.split(","))
      .map(t => Person(t(0), t(1).trim.toInt))
      .toDF()

    df.createOrReplaceTempView("pt")

    val sdf = spark.sql("select name, age from pt where age > 10")
    sdf.show()

    sdf.map(row => "name:" + row(0)).show()

    sdf.map(row => "name:" + row.getAs[String]("name")).show()

    implicit val encoder = Encoders.kryo[Map[String, Any]]
    sdf.map(row => row.getValuesMap[Any](List("name", "age"))).show()



  }
}
