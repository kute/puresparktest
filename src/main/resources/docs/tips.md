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

6. Configuration

- http://spark.apache.org/docs/latest/configuration.html#spark-configuration

7. 
- SparkContext is managed on the driver
- everything that happens inside transformations is executed on the workers. Each worker have access only to its own part of the data and don't communicate with other workers**.
- Spark tolerant mechanism：Lineage（血统）和 Checkpoint（数据检查点）
http://www.jianshu.com/p/99ebcc7c92d3
- 对于有聚集函数的操作如sum ave，reduceByKey比groupByKey有更好的性能，因为reduct可以在移动每个分区之前，先按key合并本分区的数据；而groupbykey没有可接收的函数参数，需要把所有分区的数据都移到一个分区中，然后再合并，开销较大




    
