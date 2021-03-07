package spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;


public class UpdateStateByKeyWordCount {

    public static void main(String[] args) throws Exception {
        SparkConf conf = new SparkConf()
                .setMaster("local[2]")
                .setAppName("UpdateStateByKeyWordCount");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));

        jssc.sparkContext().setLogLevel("WARN");

        jssc.checkpoint("test/wordcount_checkpoint");

        JavaReceiverInputDStream<String> lines = jssc.socketTextStream("192.168.1.1", 9999);

        JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {

            @Override
            public Iterator<String> call(String line) throws Exception {
                return Arrays.asList(line.split(" ")).iterator();
            }
        });


        JavaPairDStream<String, Integer> pairs = words.mapToPair(
                new PairFunction<String, String, Integer>() {

                    @Override
                    public Tuple2<String, Integer> call(String word)
                            throws Exception {
                        return new Tuple2<String, Integer>(word, 1);
                    }
                });

        JavaPairDStream<String, Integer> wordCounts = pairs.updateStateByKey(new Function2<List<Integer>, Optional<Integer>, Optional<Integer>>() {

            @Override
            public Optional<Integer> call(List<Integer> values, Optional<Integer> state) throws Exception {
                Integer newValue = 0;

                // 从state中取出之前batch的结果，即最新累加值
                if(state.isPresent()) {
                    newValue = state.get();
                }

                // 加上本批次的
                for(Integer value : values) {
                    newValue += value;
                }

                // 返回两者之和
                return Optional.of(newValue);
            }

        });


        wordCounts.print();

        jssc.start();
        jssc.awaitTermination();


    }
}
