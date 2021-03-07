package spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;

public class SparkStreamDemo {

    public static void main(String[] args) throws Exception {
        // create a spark with 2 working thread and interval of 20 second
        SparkConf conf = new SparkConf()
                .setMaster("local[2]")
                .setAppName("spark stream demo");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(20));

        // read input data
        JavaReceiverInputDStream<String> input = jssc.socketTextStream("192.168.1.1", 9999);

        // demo
        input.flatMap((FlatMapFunction<String, String>) line -> Arrays.asList(line.split(" ")).iterator())
                .mapToPair(word -> new Tuple2<>(word, 1))
                .reduceByKey((a, b) -> a+b)
                .print();

        // start job
        jssc.start();
        jssc.awaitTermination();
    }
}
