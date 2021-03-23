package flink;

import bean.WordCount;
import datasource.GeneratorSimpleString;
import datasource.GeneratorWordCount;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.serialization.TypeInformationSerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import schema.WordCountSchema;
import schema.WordCountSerialization;

import java.util.Properties;

public class StreamingKafkaWordCountProducer {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.enableCheckpointing(5000);

        DataStreamSource<WordCount> dataSource = env.addSource(new GeneratorWordCount()).setParallelism(1);

        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers", "localhost:9092");

        FlinkKafkaProducer<WordCount> producer = new FlinkKafkaProducer<>(
                "localhost:9092",
                "wordcount",
                new TypeInformationSerializationSchema<WordCount>(TypeInformation.of(WordCount.class),
                        new ExecutionConfig()));

        producer.setWriteTimestampToKafka(true);

        dataSource.addSink(producer);

        env.execute("write to kafka streaming");

    }
}
