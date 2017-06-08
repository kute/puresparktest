package com.kute.sql

import java.util.Properties

import org.apache.spark.sql.{SaveMode, Encoders, SparkSession}

import scala.reflect.ClassTag

/**
 * Created by kute on 2017/6/3.
 */
class SqlTest {

}

case class Person(name: String, age: Long)

object SqlTest {
  val jsonl_file = "src/main/resources/person.jsonl"
  val json_file = "src/main/resources/person.json"
  val parquet_file = "src/main/resources/person.parquet"

  def main(args: Array[String]) {

    val spark = SparkSession.builder()
      .config("spark.executor.heartbeatInterval", "3500s")
      .config("spark.rpc.askTimeout", "600s")
      //avoid conflict with default port with spark-shell
      .config("spark.ui.port", 4041)
//      .config("spark.worker.memory", "1g")
      .appName("spark sql app")
      .master("spark://kutembp:7077")
      .getOrCreate()

    SparkSession.clearDefaultSession()

    //    basic(spark)

    //    interoperating(spark)

    //    datasourcesql(spark)

    //    runParquetSchemaMergingExample(spark)

    //    runJson(spark)

    runDataSourceWithJDBC(spark)

    spark.close()

  }

  def basic(spark: SparkSession): Unit = {
    import spark.implicits._

    val df = spark.read.json(jsonl_file)

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

    // object to dataset
    Seq(Person("kute", 19)).toDS().show()
    Seq(1, 2, 3).toDS().map(_ + 1).collect()

    //dataframe to dataset
    spark.read.json(jsonl_file).as[Person].show()
  }

  def interoperating(spark: SparkSession): Unit = {
    import spark.implicits._

    //RDD TO dataframe
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

  def datasourcesql(spark: SparkSession): Unit = {

    //通用加载/保存函数,默认格式是 parquet
    val df = spark.read.format("json").load(jsonl_file)
    //    df.printSchema()
    //    df.select("name", "age").write.mode(SaveMode.Overwrite).format("parquet").save(parquet_file)

    //等价于
    df.select("name", "age").write.mode(SaveMode.Overwrite).parquet(parquet_file)

    //保存到持久化表,与加载
    //    df.write.saveAsTable("persist_table")
    //    val df = spark.read.table("persist_table")

    val parquetDF = spark.read.parquet(parquet_file)
    parquetDF.createOrReplaceTempView("parquet_table")

    spark.sql("select * from parquet_table").show()

    //execute query in files directly
    spark.sql("select * from parquet.`src/main/resources/person.parquet`").show()
  }

  //模式合并, 代价较大
  def runParquetSchemaMergingExample(spark: SparkSession): Unit = {

    import spark.implicits._

    // Create a simple DataFrame, store into a partition directory
    val df1 = spark.sparkContext.makeRDD((1 to 5).map(i => (i, i * i))).toDF("value", "square")
    //key=1 分区发现
    df1.write.mode(SaveMode.Overwrite).parquet("data/test_table/key=1")

    val df2 = spark.sparkContext.makeRDD((6 to 10).map(i => (i, i * i))).toDF("value", "square")
    df2.write.mode(SaveMode.Overwrite).parquet("data/test_table/key=2")

    //http://spark.apache.org/docs/latest/sql-programming-guide.html#configuration
    val mergedDF = spark.read.option("mergeSchema", "true").parquet("data/test_table")
    mergedDF.printSchema()

    mergedDF.createOrReplaceTempView("mergetable")

    spark.sql("select * from mergetable").show()

  }

  def runJson(spark: SparkSession): Unit ={
    val rdd = spark.sparkContext.textFile(json_file)
    val jdf = spark.read.json(rdd)

    //    val jsonstr = """{"users": [{"name":"Michael","age":18},{"name":"Andy","age":30}],"source": "phone"}"""
    //    val rdd = spark.sparkContext.makeRDD(jsonstr::Nil)
    //    val jdf = spark.read.json(rdd)

    jdf.printSchema()

    jdf.show()
  }

  def runDataSourceWithJDBC(spark: SparkSession): Unit ={
    //    http://spark.apache.org/docs/latest/sql-programming-guide.html#jdbc-to-other-databases

    //    val jdbcDF = spark.read.format("jdbc")
    //      .option("driver", "com.mysql.jdbc.Driver")
    //      .option("url", "jdbc:mysql://localhost:3306/kutepro")
    //      .option("dbtable", "ployee")
    //      .option("user", "root")
    //      .option("password", "bailong110")
    //      .load()

    val conProperties = new Properties()
    conProperties.put("user", "root")
    conProperties.put("password", "bailong110")

    val jdbcDF = spark.read.jdbc("jdbc:mysql://localhost:3306/kutepro", "ployee", conProperties)
    jdbcDF.printSchema()

    jdbcDF.show()

    // save to database
//    jdbcDF.write.format("jdbc")
//      .option("driver", "com.mysql.jdbc.Driver")
//      .option("url", "jdbc:mysql://localhost:3306/kutepro")
//      .option("dbtable", "ployee")
//      .option("user", "root")
//      .option("password", "bailong110")
//      .save()

  }
}
