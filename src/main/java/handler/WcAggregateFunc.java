package handler;

import bean.WordCount;
import org.apache.flink.api.common.functions.AggregateFunction;

import java.util.HashMap;
import java.util.Optional;

public class WcAggregateFunc implements AggregateFunction<WordCount, HashMap<String, WordCount>, WordCount> {
    @Override
    public HashMap<String, WordCount> createAccumulator() {
        return new HashMap<>();
    }

    @Override
    public HashMap<String, WordCount> add(WordCount wordCount, HashMap<String, WordCount> accMap) {
        String word = wordCount.getWord();
        WordCount wc = accMap.get(word);
        if (wc == null) {
            accMap.put(word, wordCount);
        } else {
            // 取两数之积
            int cnt = wordCount.getCount() * wc.getCount();
            // 取最新时间
            long time = Math.max(wordCount.getCaptureTime(), wc.getCaptureTime());

            wc.setCount(cnt);
            wc.setCaptureTime(time);
        }

        return accMap;
    }

    @Override
    public WordCount getResult(HashMap<String, WordCount> outMap) {
        Optional<HashMap.Entry<String, WordCount>> max = outMap.entrySet().stream()
                .max((x1, x2) -> Integer.compare(x1.getValue().getCount(), x2.getValue().getCount()));
        return max.get().getValue();

//        WordCount maxVal = new WordCount();
//        for(HashMap.Entry<String, WordCount> entry : outMap.entrySet()){
//            WordCount wordCount = entry.getValue();
//            if (maxVal.getCount() <= wordCount.getCount()) {
//                maxVal = wordCount;
//            }
//        }
//        return maxVal;
    }

    @Override
    public HashMap<String, WordCount> merge(HashMap<String, WordCount> stringWordCountHashMap, HashMap<String, WordCount> acc1) {
        return null;
    }
}
