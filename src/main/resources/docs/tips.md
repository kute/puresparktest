1. sbt package

```
spark-submit --class "com.kute.example.BasicOperation" --master "local[2]" --driver-class-path="path/mysql-connector-java/jars/mysql-connector-java-6.0.5.jar" target/scala-2.11/puresparktest_2.11-1.0.jar
```
http://spark.apache.org/docs/latest/submitting-applications.html#launching-applications-with-spark-submit

2. standalone mode

http://spark.apache.org/docs/latest/spark-standalone.html

- cluster launch scripts
- spark environment config
- Single-Node Recovery with Local File System
- security with network for spark

3. RDD operations

- transforms: http://spark.apache.org/docs/latest/programming-guide.html#transformations
- actions: http://spark.apache.org/docs/latest/programming-guide.html#actions

4. Spark SQL

- http://spark.apache.org/docs/latest/sql-programming-guide.html
- parquet configuration: http://spark.apache.org/docs/latest/sql-programming-guide.html#configuration

5. URL
- sparkUI: http://localhost:8080/#
- spark properties: http://localhost:4040

5. Spark Streaming

- transformations on DStream: http://spark.apache.org/docs/latest/streaming-programming-guide.html#transformations-on-dstreams



    
