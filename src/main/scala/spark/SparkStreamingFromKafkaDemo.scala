package spark

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, HasOffsetRanges, KafkaUtils, LocationStrategies}


object SparkStreamingFromKafkaDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf(true).setMaster("local[*]").setAppName("spark streaming wordcount")

    conf.set("spark.streaming.stopGracefullyOnShutdown", "true")

    //环境对象，设置采集周期
    val scc: StreamingContext = new StreamingContext(conf, Seconds(10))
    // TODO: 可以通过ssc.sparkContext 来访问SparkContext或者通过已经存在的SparkContext来创建StreamingContext

    //设置检查点目录
    scc.sparkContext.setCheckpointDir("data/checkpoint")

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "use_a_separate_group_id_for_each_stream",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (true: java.lang.Boolean)
    )

    val topics = Array("test")

    // 创建数据源
    val kafkaStream = KafkaUtils.createDirectStream[String, String](
      scc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](
        topics,
        kafkaParams
      )
    )

    // 切开
    val words: DStream[String] = kafkaStream.flatMap(t => t.value().split(" "))

    val pairs = words.map(word => (word, 1))

    //有状态操作，更新状态,将所有批次（即采集周期）的词频累计
    val updatedWordCounts: DStream[(String, Int)] = pairs.updateStateByKey {
      case (seq, buffer) => {
        val sum = buffer.getOrElse(0) + seq.sum
        Option(sum)
      }
    }

    // 打印
    updatedWordCounts.print



    // Start the computation
    // 通过 streamingContext.start()来启动消息采集和处理
    scc.start()

    // Wait for the computation to terminate
    // 通过streamingContext.stop()来手动终止处理程序
    scc.awaitTermination()

  }
}
