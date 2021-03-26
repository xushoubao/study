package flink;

import bean.WordCount;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.TypeInformationSerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.apache.flink.util.Collector;
import scala.Tuple2;
import utils.GlobalConfig;

import java.util.Properties;

public class Kafka2Redis {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.enableCheckpointing(5000);

        GlobalConfig config = GlobalConfig.instance("dev", "conf");
        Properties prop = config.properties;
        prop.setProperty("group.id", "kafka2redis");

        FlinkKafkaConsumer<WordCount> consumer = new FlinkKafkaConsumer<WordCount>("watermark",
                new TypeInformationSerializationSchema<WordCount>(TypeInformation.of(WordCount.class), new ExecutionConfig()),
                prop);

        // 从kafka中读取数据
        SingleOutputStreamOperator<Tuple2<String, String>> inputStream = env.addSource(consumer).flatMap(new FlatMapFunction<WordCount, Tuple2<String, String>>() {
            @Override
            public void flatMap(WordCount value, Collector<Tuple2<String, String>> out) throws Exception {
                System.out.println(value.toString());
                out.collect(new Tuple2<String, String>(value.getWord(), value.getCount() +""));
            }
        });

        // redis的配置信息
        FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder()
                .setHost("localhost")
                .build();

        // 写到redis中
        inputStream.addSink(new RedisSink<>(conf, new RedisMapper<Tuple2<String, String>>() {
            @Override
            public RedisCommandDescription getCommandDescription() {
                return new RedisCommandDescription(RedisCommand.SET);
            }

            @Override
            public String getKeyFromData(Tuple2<String, String> data) {
                return data._1;
            }

            @Override
            public String getValueFromData(Tuple2<String, String> data) {
                return data._2;
            }
        }));

        env.execute("kafka to redis job");
    }
}
