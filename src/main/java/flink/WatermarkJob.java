package flink;

import bean.WordCount;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.serialization.TypeInformationSerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

public class WatermarkJob {

    public static void main(String[] args) {
        // setup stream env
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // set event time
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers", "localhost:9092");
        prop.setProperty("group.id", "study_flink_watermark");

        FlinkKafkaConsumer<WordCount> consumer = new FlinkKafkaConsumer<>("watermark",
                new TypeInformationSerializationSchema<WordCount>(
                        TypeInformation.of(WordCount.class),
                        new ExecutionConfig()),
                prop);

        consumer.setStartFromGroupOffsets();


//        env.addSource(consumer).assignTimestampsAndWatermarks(new )




    }
}
