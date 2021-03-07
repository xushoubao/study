package flink;

import bean.WordCount;
import handler.WcAggregateFunc;
import handler.WcAssignerWithPeriodicWatermark;
import handler.WcFlatMapFunc;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.HashMap;

@Deprecated
public class WatermarkJob {

    public static void main(String[] args) {
        // setup stream env
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // set event time
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStreamSource<String> stream = env.socketTextStream("192.168.1.1", 9999);

        stream.flatMap(new WcFlatMapFunc())
                .assignTimestampsAndWatermarks(new WcAssignerWithPeriodicWatermark())
                .keyBy((wordCount) -> wordCount.getWord())
                .timeWindow(Time.seconds(5))
                .aggregate(new WcAggregateFunc())
//                .sum("count")
                .print();
    }
}
