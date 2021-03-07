package spark

import org.apache.spark.sql.SparkSession

object SparkWordCount {

  def main(args: Array[String]): Unit = {

    // create a spark
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("Word Count")
      .getOrCreate()

    // input data
    val data = spark.read.textFile("data/test/test.txt")

    // demo1 处理逻辑
    val words = data.filter(line => line.contains("14")).count()
    println(s"contains num of 14 is $words")

    // demo2
    val wordCount = data.rdd.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)
    wordCount.foreach(x => println(s"${x._1} appears ${x._2} times"))

    // close spark
    spark.stop()

  }
}
