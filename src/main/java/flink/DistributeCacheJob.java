package flink;

import handler.ReadCacheFlatMapFunc;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.LocalEnvironment;
import org.apache.flink.api.java.operators.DataSource;

public class DistributeCacheJob {

    public static void main(String[] args) throws Exception {
        // set up env
//        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        LocalEnvironment env = ExecutionEnvironment.createLocalEnvironment();

        //register cache file
//        env.registerCachedFile("data/test/cache.txt", "distribute");

        DataSource<String> data = env.fromElements("aaa", "bbb", "ccc");

        data.flatMap(new ReadCacheFlatMapFunc())
                .setParallelism(1)
                .print();

//        env.execute("Flink distribute cache job");
    }
}
