package spark;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Tuple2;

import java.util.*;

public class SparkStreamFromKafka {

    public static void main(String[] args) throws Exception {
        // create a spark with 2 working thread and interval of 5 second
        SparkConf conf = new SparkConf()
                .setMaster("local[2]")
                .setAppName("spark stream demo");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));

        jssc.checkpoint("test/checkpoint");
        jssc.sparkContext().setLogLevel("WARN");

        Set<String> topics = new HashSet<>();
        topics.add("test");

        Map<String, Object> param = new HashMap<>();
        param.put("metadata.broker.list", "192.168.1.1:9092,192.168.1.2:9092,192.168.1.3:9092");
        param.put("bootstrap.servers", "192.168.1.1:9092,192.168.1.2:9092,192.168.1.3:9092");
        param.put("group.id", "test");
        param.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        param.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        JavaInputDStream<ConsumerRecord<String, String>> inputDStream = KafkaUtils.createDirectStream(jssc,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.Subscribe(topics, param));

        JavaDStream<String> wordsDStream = inputDStream.flatMap(new FlatMapFunction<ConsumerRecord<String, String>, String>() {

            @Override
            public Iterator<String> call(ConsumerRecord<String, String> record) throws Exception {
                return Arrays.asList(record.value().split(" ")).iterator();
            }
        });

        JavaPairDStream<String, Integer> wordPairDStream = wordsDStream.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<>(s, 1);
            }
        });

        // 无状态
//        wordPairDStream.reduceByKey(new Function2<Integer, Integer, Integer>() {
//            @Override
//            public Integer call(Integer v1, Integer v2) throws Exception {
//                return v1 + v2;
//            }
//        }).print();

        // 有状态
        JavaPairDStream<String, Integer> result = wordPairDStream.updateStateByKey((Function2<List<Integer>, Optional<Integer>, Optional<Integer>>) (values, state) -> {

            int val = 0;

            if (state.isPresent()){
                val = state.get();
            }

            for (Integer value : values) {
                val += value;
            }
            return Optional.of(val);
        });

        result.print();

        jssc.start();

        jssc.awaitTermination();
    }
}
