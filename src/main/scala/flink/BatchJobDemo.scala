package flink

import org.apache.flink.api.scala.ExecutionEnvironment


object BatchJobDemo {

  def main(args: Array[String]): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment

    import org.apache.flink.api.scala._
    val input = env.fromElements("this", "is", "this", "test")

    input.map(w => (w, 1)).groupBy(0).sum(1).print()

//    env.execute("batch job demo")
  }

}
