package bean;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class WordCount {
    private String word;
    private int count;
    private long captureTime;
    private final static String[] arr = {"a", "b", "c", "d", "e", "f", "g", "h", "i", "j"};

    public WordCount() {

    }

    public WordCount(String word, int count) {
        this.word = word;
        this.count = count;
        this.captureTime = System.currentTimeMillis() / 1000;
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
        return "word: "+ word +",count: "+ count +",time: "+ captureTime;
    }

    public static WordCount genWc() {
        int cnt  = new Random().nextInt(arr.length);
        String word = arr[cnt];
        return new WordCount(word, cnt);
    }

    public static void main(String[] args) throws InterruptedException {
        List<String> res = new ArrayList<>();
        for (int i = 0; i < 50 ; i++) {
            res.add(WordCount.genWc().toString());
            Thread.sleep(100);
        }
        res.forEach(System.out::println);
    }
}
