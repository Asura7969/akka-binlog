package com.asura.bigdata.debezium;

import com.alibaba.fastjson2.JSON;
import com.asura.bigdata.model.Event;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.json.JsonConverterConfig;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.storage.ConverterConfig;
import org.apache.kafka.connect.storage.ConverterType;

import java.util.HashMap;

import static com.alibaba.fastjson2.JSONReader.Feature.FieldBased;
import static com.alibaba.fastjson2.JSONReader.Feature.IgnoreSetNullValue;

public class JsonDebeziumDeserializationSchema implements DebeziumDeserializationSchema<Event>{

    private static final long serialVersionUID = 1L;

    private transient JsonConverter jsonConverter;

    private final Boolean includeSchema;

    public JsonDebeziumDeserializationSchema() {
        this(false);
    }

    public JsonDebeziumDeserializationSchema(Boolean includeSchema) {
        this.includeSchema = includeSchema;
    }


    @Override
    public Event deserialize(SourceRecord record) throws Exception {
        if (jsonConverter == null) {
            // initialize jsonConverter
            jsonConverter = new JsonConverter();
            final HashMap<String, Object> configs = new HashMap<>(2);
            configs.put(ConverterConfig.TYPE_CONFIG, ConverterType.VALUE.getName());
            configs.put(JsonConverterConfig.SCHEMAS_ENABLE_CONFIG, includeSchema);
            jsonConverter.configure(configs);
        }
        byte[] bytes =
                jsonConverter.fromConnectData(record.topic(), record.valueSchema(), record.value());
        return JSON.parseObject(bytes, Event.class, IgnoreSetNullValue, FieldBased);
    }
}
