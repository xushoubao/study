package flink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import schema.CustomDeserialization;

import java.util.Properties;

public class StreamingKafkaCustomConsumer {

    private static Logger logger = LoggerFactory.getLogger(StreamingKafkaCustomConsumer.class);

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.enableCheckpointing(5000);

        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers", "localhost:9092");
        prop.setProperty("group.id", "study_flink_custom");

        //自定义方式读取消息
        FlinkKafkaConsumer<ConsumerRecord<String, String>> consumer = new FlinkKafkaConsumer<>(
                "test",
                new CustomDeserialization(),
                prop);

        // 从上次结束的地方开始消费
        consumer.setStartFromGroupOffsets();


        env.addSource(consumer).flatMap(new FlatMapFunction<ConsumerRecord<String,String>, String>() {
            @Override
            public void flatMap(ConsumerRecord<String, String> value, Collector<String> out) throws Exception {
                if (value != null) {
                    logger.info("record topic {}, partition {}, offset {}， message {}",
                            value.topic(), value.partition(), value.offset(), value.value());
                    out.collect(value.value());
                }
            }
        });

        env.execute("read custom message from kafka");


    }
}
