package spark

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreamingDemo {

  def main(args: Array[String]): Unit = {
    // create spark with 3 working thread and interval of 10 seconds
    val conf = new SparkConf().setMaster("local[3]").setAppName("spark streaming demo")
    val ssc = new StreamingContext(conf, Seconds(10))

    // read input data
    val input = ssc.socketTextStream("192.168.1.1", 9999)


    val wordCount = input.flatMap(_.split(" "))
      .map(word => (word, 1))
      .reduceByKey((a, b) => a+b).print()


    // start a job
    ssc.start()
    ssc.awaitTermination()
  }

}
