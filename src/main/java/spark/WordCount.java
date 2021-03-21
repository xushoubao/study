package spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

public class WordCount {
    public static void main(String[] args) {
        // create a spark conn
        SparkConf conf = new SparkConf()
                .setMaster("local")
                .setAppName("word count");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // input data
        JavaRDD<String> data = sc.textFile("data/test/test.txt", 1).cache();

        // demo1
        long count = data.filter(line -> line.contains("14")).count();
        System.out.println("file contains [14] num is "+ count);

        // demo2
        JavaRDD<String> lines2Words = data.flatMap(word -> {
            String[] words = word.split(" ");
            return Arrays.asList(words).iterator();
        });

        lines2Words.mapToPair(s -> new Tuple2<>(s, 1))
                .reduceByKey((integer, integer2) -> integer + integer2)
                .foreach(tuple2 -> System.out.println(tuple2._1 +" appear "+ tuple2._2 + " times."));

        // close spark
        sc.stop();
    }
}
