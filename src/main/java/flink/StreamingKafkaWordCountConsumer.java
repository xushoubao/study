package flink;

import bean.WordCount;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.TypeInformationSerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

public class StreamingKafkaWordCountConsumer {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.enableCheckpointing(5000);

        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers", "localhost:9092");
        prop.setProperty("group.id", "study_flink_word_count");

        FlinkKafkaConsumer<WordCount> consumer = new FlinkKafkaConsumer<WordCount>(
                "watermark",
                new TypeInformationSerializationSchema(TypeInformation.of(WordCount.class), new ExecutionConfig()),
                prop);

        // 5.从上次结束的地方开始消费
        consumer.setStartFromGroupOffsets();


        env.addSource(consumer).flatMap((FlatMapFunction<WordCount, String>) (value, out) -> {
            out.collect(value.toString());
            System.out.println(value);
        }).returns(String.class);

        env.execute("read message from kafka");


    }
}
