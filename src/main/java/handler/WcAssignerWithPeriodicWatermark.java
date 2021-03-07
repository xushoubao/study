package handler;

import bean.WordCount;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;

public class WcAssignerWithPeriodicWatermark implements AssignerWithPeriodicWatermarks<WordCount> {
    private long maxDelay = 5000L;
    private long currentTime = 0L;

    @Nullable
    @Override
    public Watermark getCurrentWatermark() {
        return new Watermark(currentTime - maxDelay);
    }

    @Override
    public long extractTimestamp(WordCount wordCount, long l) {
        if (currentTime < wordCount.getCount()) {
            currentTime = wordCount.getCaptureTime();
        }
        return wordCount.getCaptureTime();
    }
}
