package bean;

import java.util.Map;
import java.util.Objects;

/**
 * 数据的增删改
 * todo: 表结构的创建, 更改（遗留，表结构的更改，目前需要人工介入）
 */
public class CdcRecord {

    String source; // mysql, oracle, mongo

    String database;

    String table;

    String type;  // insert, delete, update

    Map<String, Object> primaryKey;

    Map<String, Object> beforeSchema;

    Map<String, Object> afterSchema;

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public Map<String, Object> getPrimaryKey() {
        return primaryKey;
    }

    public void setPrimaryKey(Map<String, Object> primaryKey) {
        this.primaryKey = primaryKey;
    }

    public Map<String, Object> getBeforeSchema() {
        return beforeSchema;
    }

    public void setBeforeSchema(Map<String, Object> beforeSchema) {
        this.beforeSchema = beforeSchema;
    }

    public Map<String, Object> getAfterSchema() {
        return afterSchema;
    }

    public void setAfterSchema(Map<String, Object> afterSchema) {
        this.afterSchema = afterSchema;
    }

    @Override
    public String toString() {
        return "CdcRecord{" +
                "source='" + source + '\'' +
                ", database='" + database + '\'' +
                ", table='" + table + '\'' +
                ", type='" + type + '\'' +
                ", primaryKey=" + primaryKey +
                ", beforeSchema=" + beforeSchema +
                ", afterSchema=" + afterSchema +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CdcRecord record = (CdcRecord) o;
        return Objects.equals(source, record.source)
                && Objects.equals(database, record.database)
                && Objects.equals(table, record.table)
                && Objects.equals(type, record.type)
                && Objects.equals(primaryKey, record.primaryKey)
                && Objects.equals(beforeSchema, record.beforeSchema)
                && Objects.equals(afterSchema, record.afterSchema);
    }

    @Override
    public int hashCode() {
        return Objects.hash(source, database, table, type, primaryKey, beforeSchema, afterSchema);
    }
}
