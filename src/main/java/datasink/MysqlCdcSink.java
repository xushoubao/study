package datasink;

import bean.CdcRecord;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Map;
import java.util.Properties;

/**
 * 向mysql写入数据
 * todo: 不能识别字段类型（遗留，目前建表全部使用text类型）
 */
public class MysqlCdcSink extends RichSinkFunction<CdcRecord> {

    private static final Logger logger = LoggerFactory.getLogger(MysqlCdcSink.class);

    private String driver;
    private String url;
    private String user;
    private String password;
    private Connection connection = null;

    public MysqlCdcSink(Properties properties) {
        this.driver = properties.getOrDefault("output.driver", "com.mysql.jdbc.Driver").toString();
        this.url = properties.getOrDefault("output.url", "jdbc:mysql://47.110.230.144:8306/warehouse?useUnicode=true&characterEncoding=UTF-8&useSSL=false").toString();
        this.user = properties.getOrDefault("output.user", "mpo").toString();
        this.password = properties.getOrDefault("output.password", "mponline").toString();
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        Class.forName(driver);
        connection = DriverManager.getConnection(url, user, password);
        connection.setAutoCommit(false);
        logger.info("create jdbc connection success.");
    }

    @Override
    public void invoke(CdcRecord cdcRecord, Context context) throws Exception {

        Statement statement = connection.createStatement();

        Map<String, Object> beforeSchema = cdcRecord.getBeforeSchema();
        if (beforeSchema != null) { // delete from db
            String sql = deleteSql(cdcRecord);
            if (statement.execute(sql)) {
                logger.debug("sql[{}] exec success.", sql);
            }

        }

        Map<String, Object> afterSchema = cdcRecord.getAfterSchema();
        if (afterSchema != null) { // insert into db
            String sql = insertSql(cdcRecord);
            if (statement.execute(sql)) {
                logger.debug("sql[{}] exec success.", sql);
            }
        }

        connection.commit();
    }

    private String insertSql(CdcRecord cdcRecord) {
        Map<String, Object> afterSchema = cdcRecord.getAfterSchema();
        if (afterSchema == null) {
            return null;
        }

        StringBuilder name = new StringBuilder();
        StringBuilder value = new StringBuilder();
        int count = 0;
        for (Map.Entry<String, Object> entry : afterSchema.entrySet()) {
            String columnName = entry.getKey();
            Object columnValue = entry.getValue();
            if(count != 0) {
                name.append(", ");
                value.append(", ");
            }
            name.append(columnName);
            value.append("'").append(columnValue).append("'");

            count++;
        }

        return String.format(" insert into warehouse.%s (%s) values (%s)", cdcRecord.getTable(), name, value);
    }

    private String deleteSql(CdcRecord cdcRecord) {
        Map<String, Object> primaryKey = cdcRecord.getPrimaryKey();
        if (primaryKey == null) {
            return null;
        }

        StringBuilder condition = new StringBuilder();
        int count = 0;
        for (Map.Entry<String, Object> entry : primaryKey.entrySet()) {
            String columnName = entry.getKey();
            Object columnValue = entry.getValue();
            if (count != 0) {
                condition.append(" and ");
            }
            condition.append(columnName).append(" = ").append("'").append(columnValue).append("'");

            count++;
        }

        return String.format(" delete from warehouse.%s where %s ", cdcRecord.getTable(), condition);
    }

    @Override
    public void close() throws Exception {
        super.close();
        connection.close();
    }
}
