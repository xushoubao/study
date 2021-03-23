package datasource;

import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class GeneratorSimpleString implements ParallelSourceFunction<String> {

    private volatile boolean isRunning = true;
    private volatile int serNo = 0;
    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        while (isRunning) {


            List<String> list = new ArrayList<>();
            list.add("锄禾日当午 --> "+ serNo +".chrdw");
            list.add("汗滴禾下土 --> "+ serNo +".hdhxt");
            list.add("谁知盘中餐 --> "+ serNo +".szpzc");
            list.add("粒粒皆辛苦 --> "+ serNo +".lljxk");
            serNo++;

            int index = new Random().nextInt(list.size());

            String text = list.get(index);
            ctx.collect(text);
            System.out.println(text);

            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;

    }
}
