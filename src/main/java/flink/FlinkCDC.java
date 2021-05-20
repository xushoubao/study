package flink;

import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.alibaba.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Properties;

public class FlinkCDC {

    public static void main(String[] args) throws Exception {
        //TODO 1.获取流处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        /* 读取的是binlog中的数据，如果集群挂掉，尽量能实现断点续传功能。如果从最新的读取（丢数据）。如果从最开始读（重复数据）。
        理想状态：读取binlog中的数据读一行，保存一次读取到的（读取到的行）位置信息。而flink中读取行位置信息保存在Checkpoint中。
        使用Checkpoint可以把flink中读取（按行）的位置信息保存在Checkpoint中 */

        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // 5s ck一次
        env.enableCheckpointing(5 * 1000L);
        //任务挂掉的时候是否清理checkpoint。使任务正常退出时不删除CK内容，有助于任务恢复。默认的是取消的时候清空checkpoint中的数据。RETAIN_ON_CANCELLATION表示取消任务的时候，保存最后一次的checkpoint。便于任务的重启和恢复，正常情况下都使用RETAIN
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //设置一个状态后端 jobManager。如果使用的yarn集群模式，jobManager随着任务的生成而生成，任务挂了jobManager就没了。因此需要启动一个状态后端。只要设置checkpoint，尽量就设置一个状态后端。保存在各个节点都能读取的位置：hdfs中
        env.setStateBackend(new FsStateBackend("file:///Users/bao/code/private/study/data/checkpoint"));

        //设置一个重启策略：默认的固定延时重启次数，重启的次数是Integer的最大值，重启的间隔是2s
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 2000L));


        //创建一个变量可以添加之后想添加的配置信息
        Properties properties = new Properties();
        System.out.println("properties is "+ properties);

        // 读取mysql变化数据 监控MySQL中变化的数据
        DebeziumSourceFunction<String> sourceFunction = MySQLSource.<String>builder() //使用builder创建MySQLsource对象，需要指定对象的泛型。
                .hostname("47.110.230.144") //指定监控的哪台服务器（MySQL安装的位置）
                .port(8306) //MySQL连接的端口号
                .username("mpo") //用户
                .password("mponline")//密码
                .databaseList("analysis, datax_product") //list：可以监控多个库
                .tableList("analysis.person, analysis.person_new, datax_product.person") //如果不写则监控库下的所有表，需要使用【库名.表名】
                .debeziumProperties(properties) //debezium中有很多配置信息。可以创建一个对象来接收
                .deserializer(new StringDebeziumDeserializationSchema()) //读的数据是binlog文件，反序列化器，解析数据
                .startupOptions(StartupOptions.latest())
                .build();

        DataStreamSource<String> streamSource = env.addSource(sourceFunction);


        SingleOutputStreamOperator<String> map = streamSource.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> collector) throws Exception {

                System.out.println(" receive a record: "+ value);
                collector.collect(value.toLowerCase());
            }
        }).name("myCustomerFlatMap");

        // 启动任务
        env.execute("flink cdc stream job");

    }
}
