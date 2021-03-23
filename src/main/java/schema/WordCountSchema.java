package schema;

import bean.WordCount;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;

public class WordCountSchema implements SerializationSchema<WordCount>, DeserializationSchema<WordCount> {
    private ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public WordCount deserialize(byte[] message) throws IOException {
        return objectMapper.readValue(message, WordCount.class);
    }

    @Override
    public boolean isEndOfStream(WordCount nextElement) {
        return false;
    }

    @Override
    public byte[] serialize(WordCount element) {
        try {
            return objectMapper.writeValueAsBytes(element);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return new byte[0];
    }

    @Override
    public TypeInformation<WordCount> getProducedType() {
        return TypeInformation.of(WordCount.class);
    }
}
