package flink

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

object StreamingJobDemo {

  def main(args: Array[String]): Unit = {

    // create stream env
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // input stream
    val inputStream = env.socketTextStream("192.168.1.1", 9999)

    // process
    inputStream.print()


    // exec stream
    env.execute("word count streaming")
  }

}
