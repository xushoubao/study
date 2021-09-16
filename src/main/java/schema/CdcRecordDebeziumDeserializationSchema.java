package schema;

import bean.CdcRecord;
import com.alibaba.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class CdcRecordDebeziumDeserializationSchema implements DebeziumDeserializationSchema<CdcRecord> {

    private static final Logger logger = LoggerFactory.getLogger(CdcRecordDebeziumDeserializationSchema.class);

    private static final int DATA_SOURCE_INDEX = 0;
    private static final int DATABASE_INDEX = 1;
    private static final int TABLE_INDEX = 2;

    private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    @Override
    public void deserialize(SourceRecord sourceRecord, Collector<CdcRecord> collector) throws Exception {

        logger.debug("receive a record: {}", sourceRecord);

        CdcRecord record = new CdcRecord();

        String topic = sourceRecord.topic();
        String[] split = topic.split("\\.", -1);
        if (split.length == 3) {
            record.setSource(split[DATA_SOURCE_INDEX]);
            record.setDatabase(split[DATABASE_INDEX]);
            record.setTable(split[TABLE_INDEX]);
        }

        Struct struct = (Struct) sourceRecord.value();
        Struct before = struct.getStruct("before");
        if (before != null) {
            record.setBeforeSchema(parseSchema(before));
        }

        Struct after = struct.getStruct("after");
        if (after != null) {
            record.setAfterSchema(parseSchema(after));
        }

        Struct key = (Struct) sourceRecord.key();
        if (key != null) {
            record.setPrimaryKey(parseSchema(key));
        }

        Envelope.Operation operation = Envelope.operationFor(sourceRecord);
        String type = operation.name();
        record.setType(type);

        logger.debug("trans a record: {}", record);

        collector.collect(record);
    }

    private Map<String, Object> parseSchema(Struct struct) {
        Map<String, Object> map = new HashMap<>();
        Schema schema = struct.schema();
        for (Field field : schema.fields()) {
            String name = field.name();
            Object value = struct.get(name);
            String fieldType = field.schema().name();
            if ("io.debezium.time.Timestamp".equals(fieldType)) {
                value = sdf.format(new Date(Long.parseLong(value.toString()) - 8 * 3600 * 1000));
            }
            map.put(name, value);
        }
        return map;
    }

    @Override
    public TypeInformation<CdcRecord> getProducedType() {
        return TypeInformation.of(CdcRecord.class);
    }
}
