package flink;

import handler.ReadCacheFlatMapFunc;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;

public class DistributeCacheJob {

    public static void main(String[] args) throws Exception {
        // set up env
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //register cache file
        env.registerCachedFile("src/main/resources/cache.txt", "distribute");

        DataSource<String> data = env.fromElements("aaa", "bbb", "ccc");

        data.flatMap(new ReadCacheFlatMapFunc()).setParallelism(1).print();

//        env.execute("Flink distribute cache job");
    }
}
