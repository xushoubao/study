package flink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Properties;

public class StreamingKafkaConsumer {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.enableCheckpointing(5000);

        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers", "localhost:9092");
        // kakfa 0.8及以前的版本需要从zookeeper中获取偏移量
//        prop.setProperty("zookeeper.connect", "localhost:2181");
        prop.setProperty("group.id", "study_flink");

        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<String>(
                "test",
                new SimpleStringSchema(),
                prop);

        // kafka消费数据offset的方法，有物种
        // 1.从最早的数据开始
//        consumer.setStartFromEarliest();
        // 2.从指定时间开始
//        consumer.setStartFromTimestamp(1654327890L);
        // 3.从最新的数据开始
//        consumer.setStartFromLatest();

        // 4.手动指定相应的topic,partition,offset,然后从指定处消费
//        HashMap<KafkaTopicPartition, Long> map = new HashMap<>();
//        map.put(new KafkaTopicPartition("test", 1), 100L);
//        map.put(new KafkaTopicPartition("test", 2), 103L);
//        consumer.setStartFromSpecificOffsets(map);

        // 5.从上次结束的地方开始消费
        consumer.setStartFromGroupOffsets();


        env.addSource(consumer).flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                String[] words = value.split(" ", -1);
                System.out.println(value);
                for (String word : words) {
                    out.collect(word);
                }
            }
        });

        env.execute("read message from kafka");


    }
}
