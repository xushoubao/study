package flink;

import datasource.GeneratorSimpleString;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

public class StreamingKafkaProducer {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.enableCheckpointing(5000);

        DataStreamSource<String> dataSource = env.addSource(new GeneratorSimpleString()).setParallelism(1);

        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers", "localhost:9092");

        FlinkKafkaProducer<String> producer = new FlinkKafkaProducer<>(
                "localhost:9092",
                "test",
                new SimpleStringSchema());

        producer.setWriteTimestampToKafka(true);

        dataSource.addSink(producer);

        env.execute("write to kafka streaming");

    }
}
