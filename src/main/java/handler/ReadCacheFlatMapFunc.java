package handler;

import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.util.Collector;

import java.io.File;
import java.util.List;

public class ReadCacheFlatMapFunc extends RichFlatMapFunction<String, String> {
    @Override
    public void flatMap(String s, Collector<String> collector) throws Exception {
        //fetch from cache memory
        File file = getRuntimeContext().getDistributedCache().getFile("distribute");
        List<String> lines = FileUtils.readLines(file);
        lines.forEach(System.out::println);
    }
}
