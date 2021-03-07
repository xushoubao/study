package datasource;

import bean.WordCount;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Generator implements SourceFunction<WordCount> {
    private Logger logger = LoggerFactory.getLogger(this.getClass());
    private boolean running = true;

    @Override
    public void run(SourceContext<WordCount> sourceContext) throws Exception {
        while (running) {
            WordCount wordCount = WordCount.genWc();
            System.out.println(wordCount.toString());
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
