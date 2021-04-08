package bean;

public class WordCount {

    private String batchId;
    private String word;
    private int count;
    private long captureTime;

    public WordCount() {
    }

    public WordCount(String word, int count, long captureTime) {
        this.word = word;
        this.count = count;
        this.captureTime = captureTime;
    }

    public WordCount(String batchId, String word, int count, long captureTime) {
        this.batchId = batchId;
        this.word = word;
        this.count = count;
        this.captureTime = captureTime;
    }

    public String getBatchId() {
        return batchId;
    }

    public void setBatchId(String batchId) {
        this.batchId = batchId;
    }

    public String getWord() {
        return word;
    }

    public void setWord(String word) {
        this.word = word;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public void setCaptureTime(long captureTime) {
        this.captureTime = captureTime;
    }

    public long getCaptureTime() {
        return captureTime;
    }

    @Override
    public String toString() {
        return "WordCount{" +
                "batchId='" + batchId + '\'' +
                ", word='" + word + '\'' +
                ", count=" + count +
                ", captureTime=" + captureTime +
                '}';
    }
}
