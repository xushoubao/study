package schema;

import bean.WordCount;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;

@Deprecated
public class WordCountDeserialization implements DeserializationSchema<WordCount> {

    ObjectMapper objectMapper = new ObjectMapper();
    @Override
    public WordCount deserialize(byte[] message) throws IOException {
        return objectMapper.readValue(message, WordCount.class);
    }

    @Override
    public boolean isEndOfStream(WordCount nextElement) {
        return false;
    }

    @Override
    public TypeInformation<WordCount> getProducedType() {
        return TypeInformation.of(WordCount.class);
    }
}
