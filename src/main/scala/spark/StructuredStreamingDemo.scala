package spark

import org.apache.spark.sql.SparkSession

object StructuredStreamingDemo {

  def main(args: Array[String]): Unit = {
    // create spark
    val spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("structured streaming demo")
      .getOrCreate()

    import spark.implicits._

    val line = spark.readStream
      .format("socket")
      .option("host", "192.168.1.1")
      .option("port", "9999")
      .load()

    line.printSchema()

    val word = line.as[String].flatMap(_.split(" "))
//    word.withWatermark("value", "10 second")
    val wordCount = word.groupBy("value").count()


    // output to console
    val query = wordCount.writeStream
      .outputMode("update")
      .format("console")
      .start()

    query.awaitTermination()
  }

}
