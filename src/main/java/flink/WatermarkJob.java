package flink;

import bean.WordCount;
import groovy.lang.Closure;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.TypeInformationSerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import utils.GlobalConfig;

import java.time.Duration;
import java.util.Properties;

public class WatermarkJob {

    public static void main(String[] args) throws Exception {

        // 创建流执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置事件时间
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // kafka的参数
        Properties properties = GlobalConfig.instance("dev", "conf").properties;
        String groupId = (String) ((Closure) properties.get("group.id")).call("watermark");
        properties.setProperty("group.id", groupId);

        // 设置消费分组参数
        FlinkKafkaConsumer<WordCount> consumer = new FlinkKafkaConsumer<>("watermark",
                new TypeInformationSerializationSchema<WordCount>(
                        TypeInformation.of(WordCount.class),
                        new ExecutionConfig()),
                properties);
        consumer.setStartFromGroupOffsets();

        // 输入数据
        DataStreamSource<WordCount> inputStream = env.addSource(consumer);

        // 处理逻辑
        SingleOutputStreamOperator<WordCount> processStream = inputStream.assignTimestampsAndWatermarks(WatermarkStrategy.<WordCount>forBoundedOutOfOrderness(Duration.ofSeconds(5)) .withTimestampAssigner(new SerializableTimestampAssigner<WordCount>() {
            @Override
            public long extractTimestamp(WordCount element, long recordTimestamp) {
                return element.getCaptureTime() * 1000L; // 注意这里是毫秒，否则时间会有问题
            }
        })).keyBy(new KeySelector<WordCount, String>() {
            @Override
            public String getKey(WordCount value) throws Exception {
                return value.getWord();
            }
        }).window(TumblingEventTimeWindows.of(Time.seconds(20))).allowedLateness(Time.seconds(10)).reduce(new ReduceFunction<WordCount>() {
            @Override
            public WordCount reduce(WordCount value1, WordCount value2) throws Exception {
                String batch;
                if (value1.getBatchId().compareTo(value2.getBatchId()) > 0) {
                    batch = value1.getBatchId();
                } else {
                    batch = value2.getBatchId();
                }

                int count = value1.getCount() + value2.getCount();

                long captureTime = Math.max(value1.getCaptureTime(), value2.getCaptureTime());

                return new WordCount(batch, value1.getWord(), count, captureTime);
            }
        });

        // 输出结果
        processStream.print();

        // 提交执行
        env.execute("watermark stream job");
    }
}
