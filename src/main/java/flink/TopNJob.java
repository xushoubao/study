package flink;

import bean.WordCount;
import groovy.lang.Closure;
import groovy.lang.Tuple2;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.TypeInformationSerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.apache.flink.util.Collector;
import utils.GlobalConfig;

import java.util.*;

/**
 * 从kafka中实时消费数据，统计topN
 */
public class TopNJob {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = GlobalConfig.instance("dev", "conf").properties;
        String groupId = (String) ((Closure) properties.get("group.id")).call("topN");
        properties.setProperty("group.id", groupId);
        FlinkKafkaConsumer<WordCount> consumer = new FlinkKafkaConsumer<>("watermark",
                new TypeInformationSerializationSchema<WordCount>(
                        TypeInformation.of(WordCount.class),
                        new ExecutionConfig()),
                properties);
        consumer.setStartFromGroupOffsets();

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.enableCheckpointing(60 * 1000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(30 * 1000);

        SingleOutputStreamOperator<Tuple2<String, Integer>> processStream = env.addSource(consumer)
                .assignTimestampsAndWatermarks(WatermarkStrategy.<WordCount>noWatermarks()
                        .withTimestampAssigner(new SerializableTimestampAssigner<WordCount>() {
                            @Override
                            public long extractTimestamp(WordCount element, long recordTimestamp) {
                                return element.getCaptureTime() * 1000L;
                            }
                        }))
                .keyBy(new KeySelector<WordCount, String>() {
                    @Override
                    public String getKey(WordCount value) throws Exception {
                        return value.getWord();
                    }
                })
                .window(SlidingEventTimeWindows.of(Time.seconds(60), Time.seconds(10)))
                .reduce(new ReduceFunction<WordCount>() {
                    @Override
                    public WordCount reduce(WordCount value1, WordCount value2) throws Exception {
                        String batchId;
                        if (value1.getBatchId().compareTo(value2.getBatchId()) > 0) {
                            batchId = value1.getBatchId();
                        } else {
                            batchId = value2.getBatchId();
                        }
                        String word = value1.getWord();
                        int count = value1.getCount() + value2.getCount();
                        long time = Math.max(value1.getCaptureTime(), value2.getCaptureTime());
                        return new WordCount(batchId, word, count, time);
                    }
                })
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(10)))
                .process(new ProcessAllWindowFunction<WordCount, Tuple2<String, Integer>, TimeWindow>() {
                    @Override
                    public void process(Context context, Iterable<WordCount> elements, Collector<Tuple2<String, Integer>> out) throws Exception {

                        TreeSet<WordCount> treeSet = new TreeSet<>(new Comparator<WordCount>() {
                            @Override
                            public int compare(WordCount o1, WordCount o2) {
                                return o1.getCount() - o2.getCount();
                            }
                        });

                        Iterator<WordCount> iterator = elements.iterator();
                        if (iterator.hasNext()) {
                            WordCount wordCount = iterator.next();

                            if (treeSet.size() < 10) {
                                treeSet.add(wordCount);
                            } else {

                            }
                        }





                    }
                });

        // 输出到redis
        String host = properties.getProperty("host");
        String port = properties.getProperty("port", "6379");

        FlinkJedisPoolConfig config = new FlinkJedisPoolConfig.Builder()
                .setHost(host)
                .setPort(Integer.valueOf(port))
                .build();

        processStream.addSink(new RedisSink<Tuple2<String, Integer>>(config, new RedisMapper<Tuple2<String, Integer>>() {
            @Override
            public RedisCommandDescription getCommandDescription() {
                return new RedisCommandDescription(RedisCommand.SET);
            }

            @Override
            public String getKeyFromData(Tuple2<String, Integer> data) {
                return data.getV1();
            }

            @Override
            public String getValueFromData(Tuple2<String, Integer> data) {
                return String.valueOf(data.getV2());
            }
        }));


        env.execute("topN job");
    }


}
