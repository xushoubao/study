package spark

import org.apache.spark.sql.SparkSession

object SparkSqlDemo {

  case class Person(name:String, age:Long)

  def main(args: Array[String]): Unit = {
    // create spark
    val spark = SparkSession.builder().master("local").appName("sql demo").getOrCreate()

    // trans to df
    val input = spark.sparkContext.textFile("src/main/resources/people.txt")
    import spark.implicits._
    val peopleDF = input.map(line => line.split(","))
      .map(word => Person(word(0), word(1).trim.toLong))
      .toDF()

    peopleDF.createOrReplaceTempView("people")

    // demo1
    val resultDF = spark.sql("select count(*) as cnt from people")
    resultDF.show()

    // demo 2
    val ageDF = spark.sql("select sum(age) as total from people")
    ageDF.show()


    // close spark
    spark.sparkContext.stop()
    spark.close()
  }
}
