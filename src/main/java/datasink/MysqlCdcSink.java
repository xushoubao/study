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
 */
public class MysqlCdcSink extends RichSinkFunction<CdcRecord> {

    private static final Logger logger = LoggerFactory.getLogger(MysqlCdcSink.class);

    private String driver;
    private String url;
    private String user;
    private String password;
    private Connection connection = null;
    private String database;
    private String table;

    public MysqlCdcSink(Properties properties) {
        this.driver = properties.getOrDefault("output.driver", "com.mysql.jdbc.Driver").toString();
        this.url = properties.getProperty("output.url");
        this.user = properties.getProperty("output.user");
        this.password = properties.getProperty("output.password");
        this.database = properties.getProperty("output.database");
        this.table = properties.getProperty("output.table");
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        Class.forName(driver);
        connection = DriverManager.getConnection(url, user, password);
        connection.setAutoCommit(false);
        logger.info("create jdbc connection[{}] success.", connection.hashCode());
    }

    @Override
    public void invoke(CdcRecord cdcRecord, Context context) throws Exception {

        Statement statement = connection.createStatement();

        Map<String, Object> beforeSchema = cdcRecord.getBeforeSchema();
        if (beforeSchema != null) { // delete from db
            String sql = deleteSql(beforeSchema);
            if (sql != null && statement.execute(sql)) {
                logger.debug("sql[{}] exec success.", sql);
            }

        }

        Map<String, Object> afterSchema = cdcRecord.getAfterSchema();
        if (afterSchema != null) { // insert into db
            String sql = insertSql(afterSchema);
            if (sql != null && statement.execute(sql)) {
                logger.debug("sql[{}] exec success.", sql);
            }
        }

        connection.commit();
    }

    private String insertSql(Map<String, Object> schema) {
        // 动态拼接sql
        StringBuilder name = new StringBuilder();
        StringBuilder value = new StringBuilder();
        int count = 0;
        for (Map.Entry<String, Object> entry : schema.entrySet()) {
            String columnName = entry.getKey();
            Object columnValue = entry.getValue();
            if (columnValue == null) {
                continue;
            }
            if(count != 0) {
                name.append(", ");
                value.append(", ");
            }
            name.append(columnName);
            value.append("'").append(columnValue).append("'");
            count++;
        }

        // 跳过空行
        String sql = null;
        if (count != 0) {
            sql = String.format(" insert into %s.%s (%s) values (%s)", database, table, name, value);
        }

        return sql;
    }

    private String deleteSql(Map<String, Object> schema) {
        // 动态拼接条件
        StringBuilder condition = new StringBuilder();
        int count = 0;
        for (Map.Entry<String, Object> entry : schema.entrySet()) {
            String columnName = entry.getKey();
            Object columnValue = entry.getValue();
            if (columnValue == null) {
                continue;
            }
            if (count != 0) {
                condition.append(" and ");
            }
            condition.append(columnName).append(" = ").append("'").append(columnValue).append("'");
            count++;
        }

        // 跳过空行
        String sql = null;
        if (count != 0) {
            sql = String.format(" delete from %s.%s where %s ", database, table, condition);
        }

        return sql;
    }

    @Override
    public void close() throws Exception {
        super.close();
        connection.close();
    }
}
