package datasource;

import bean.WordCount;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GeneratorWordCount implements ParallelSourceFunction<WordCount> {
    private Logger logger = LoggerFactory.getLogger(this.getClass());
    private boolean running = true;

    @Override
    public void run(SourceContext<WordCount> sourceContext) throws Exception {
        while (running) {
            WordCount wordCount = WordCount.genWc();
            System.out.println("gener a data: "+ wordCount.toString());
            sourceContext.collect(wordCount);
            Thread.sleep(1000);
            logger.info("generate 1 data {}", wordCount.toString());
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
