package handler;

import bean.WordCount;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

public class WcFlatMapFunc implements FlatMapFunction<String, WordCount> {
    @Override
    public void flatMap(String in, Collector<WordCount> collector) throws Exception {
        String[] words = in.split(",");
        if (words.length == 2) {
            collector.collect(new WordCount(words[0], Integer.parseInt(words[1]), System.currentTimeMillis() / 1000));
        }
    }
}
