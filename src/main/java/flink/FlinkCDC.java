package flink;

import bean.CdcRecord;
import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import datasink.MysqlCdcSink;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import schema.CdcRecordDebeziumDeserializationSchema;
import utils.GlobalConfig;

import java.util.Properties;

public class FlinkCDC {

    private static final Logger logger = LoggerFactory.getLogger(FlinkCDC.class);

    public static void main(String[] args) throws Exception {

        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        if (!checkLaunch(parameterTool)) {
            System.out.println("Param Error");
            System.exit(1);
        }

        // 读配置
        Properties properties = GlobalConfig.instance(parameterTool.get("env"), parameterTool.get("config"), pathname -> pathname.getName().endsWith(".groovy")).properties;
        logger.info("current config is [{}]", properties);

        // 获取流处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置ck和重启策略
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.enableCheckpointing(60 * 1000L);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.setStateBackend(new FsStateBackend("file:///Users/bao/code/private/study/data/checkpoint"));
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 5000L));

        // 读取mysql变化数据 监控MySQL中变化的数据
        DebeziumSourceFunction<CdcRecord> sourceFunction = MySQLSource.<CdcRecord>builder() //使用builder创建MySQLsource对象，需要指定对象的泛型。
                .hostname(properties.getProperty("input.hostname"))
                .port(Integer.parseInt(properties.get("input.port").toString()))
                .username(properties.getProperty("input.user"))
                .password(properties.getProperty("input.password"))
                .databaseList(properties.getProperty("input.databaseList"))
                .tableList(properties.getProperty("input.tableList"))
                .deserializer(new CdcRecordDebeziumDeserializationSchema()) //读的数据是binlog文件，反序列化器，解析数据
                .startupOptions(parameterTool.get("s") == null ? StartupOptions.initial() : StartupOptions.latest())
                .build();

        DataStreamSource<CdcRecord> streamSource = env.addSource(sourceFunction).setParallelism(1);

        // 输出到mysql
        streamSource.addSink(new MysqlCdcSink(properties)).setParallelism(1);

        // 启动任务
        env.execute("FlinkCDC Stream Job");
    }

    private static boolean checkLaunch(ParameterTool parameterTool) {
        String checkpointParam = parameterTool.get("s");
        if (checkpointParam != null) {
            logger.info("s={}", checkpointParam);
        }

        String env = parameterTool.get("env");
        if (env == null) {
            System.out.println("param [--env] need");
            return false;
        }

        String config = parameterTool.get("config");
        if (config == null) {
            System.out.println("param [--config] need");
            return false;
        }

        return true;
    }
}
