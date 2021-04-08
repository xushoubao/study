package datasource;

import bean.WordCount;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 数据生成器，模拟生产数据，多线程时可以产生乱序时间的数据
 */
public class GeneratorWordCount implements ParallelSourceFunction<WordCount> {
    private Logger logger = LoggerFactory.getLogger(this.getClass());
    private boolean running = true;

    private volatile AtomicInteger batchNo = new AtomicInteger(0);

    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");
    DecimalFormat decimalFormat = new DecimalFormat("00000");

    private final String[] arr = {"a", "b", "c", "d", "e", "f", "g", "h", "i", "j"};

    public GeneratorWordCount() {
    }

    @Override
    public void run(SourceContext<WordCount> sourceContext) throws Exception {
        while (running) {
            WordCount wordCount = genWc();
            sourceContext.collect(wordCount);

            int sleepTime = new Random().nextInt(5);
            Thread.sleep(sleepTime * 1000L);
            logger.info("generate 1 data {}, sleep {} seconds", wordCount.toString(), sleepTime);
        }
    }

    public WordCount genWc() {

        String batchIdPre = this.dateFormat.format(new Date().getTime());
        String batchIdSuf = decimalFormat.format(batchNo.getAndIncrement());
        String batchId = batchIdPre + batchIdSuf;

        int index  = new Random().nextInt(arr.length);
        String word = arr[index];

        int count = new Random().nextInt(100);

        long captureTime = System.currentTimeMillis() / 1000;

        return new WordCount(batchId, word, count, captureTime);
    }

    @Override
    public void cancel() {
        running = false;
    }
}
