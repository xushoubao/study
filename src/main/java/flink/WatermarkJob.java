package flink;

import bean.WordCount;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.TypeInformationSerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.spark.streaming.Seconds;
import utils.GlobalConfig;

import java.time.Duration;
import java.util.Collection;
import java.util.Properties;

public class WatermarkJob {

    public static void main(String[] args) throws Exception {

        // 创建流执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置事件时间
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // kafka的参数
        Properties prop = GlobalConfig.instance("dev", "conf").properties;
//        prop.setProperty("group.id", "watermatkJob");
        System.out.println("config: "+ prop.toString());

        // 设置消费分组参数
        FlinkKafkaConsumer<WordCount> consumer = new FlinkKafkaConsumer<>("watermark",
                new TypeInformationSerializationSchema<WordCount>(
                        TypeInformation.of(WordCount.class),
                        new ExecutionConfig()),
                prop);
        consumer.setStartFromGroupOffsets();

        // 输入数据
        DataStreamSource<WordCount> inputStream = env.addSource(consumer);

        // 处理逻辑
        SingleOutputStreamOperator<WordCount> processStream = inputStream.assignTimestampsAndWatermarks(WatermarkStrategy.<WordCount>forBoundedOutOfOrderness(Duration.ofSeconds(5)) .withTimestampAssigner(new SerializableTimestampAssigner<WordCount>() {
            @Override
            public long extractTimestamp(WordCount element, long recordTimestamp) {
                System.out.println("receive a message: "+ element.toString());
                return element.getCaptureTime();
            }
        })).keyBy(new KeySelector<WordCount, String>() {
            @Override
            public String getKey(WordCount value) throws Exception {
                return value.getWord();
            }
        }).window(TumblingEventTimeWindows.of(Time.seconds(20))).allowedLateness(Time.seconds(10)).reduce(new ReduceFunction<WordCount>() {
            @Override // todo: 为什么没有输出，是不是时间设置的有问题？？？
            public WordCount reduce(WordCount value1, WordCount value2) throws Exception {
                return new WordCount(value1.getWord(), value1.getCount() + value2.getCount(), Math.max(value1.getCaptureTime(), value2.getCaptureTime()));
            }
        });

        // 输出结果
        processStream.print();

        // 提交执行
        env.execute("watermark stream job");
    }
}
