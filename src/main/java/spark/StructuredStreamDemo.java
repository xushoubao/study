package spark;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

import java.util.Arrays;

public class StructuredStreamDemo {
    public static void main(String[] args) throws Exception {

        // create a spark
        SparkSession spark = SparkSession.builder()
//                .master("local")
                .appName("structured stream demo")
                .getOrCreate();

        // connect to socket
        Dataset<Row> df = spark.readStream()
                .format("socket")
                .option("host", "192.168.1.1")
                .option("port", 9999)
                .load();

        df.printSchema();

        Dataset<Row> wordCount = df.as(Encoders.STRING()).flatMap((FlatMapFunction<String, String>) line -> Arrays.asList(line.split(" ")).iterator(), Encoders.STRING())
                .groupBy("value")
                .count();

        //output
        StreamingQuery query = wordCount.writeStream().outputMode("complete").format("console").start();

        // commit job
        query.awaitTermination();
    }
}
