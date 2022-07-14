package com.asura.bigdata.model;

import com.alibaba.fastjson.parser.DefaultJSONParser;
import com.alibaba.fastjson.parser.JSONToken;
import com.alibaba.fastjson.parser.deserializer.ObjectDeserializer;
import com.alibaba.fastjson.serializer.JSONSerializer;
import com.alibaba.fastjson.serializer.ObjectSerializer;
import io.debezium.data.Envelope;
import lombok.Data;
import lombok.ToString;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.lang.reflect.Type;

@ToString
@Data
public class Event {

    private Map<String, Object> before;
    private Map<String, Object> after;
    private Source source;

//        @JSONField(serializeUsing = OperationEnumSerializer.class, deserializeUsing = OperationEnumDeserializer.class)
//        private Envelope.Operation op;
    private String op;
    private Long ts_ms;


    public String getDbAndTable() {
        if (null != source) {
            return String.format("%s.%s", source.getDb(), source.getTable());
        }
        return null;
    }

    public List<Map.Entry<String, Object>> getListBefore() {
        if (null == before || before.isEmpty()) {
            return new ArrayList<>(0);
        }
        return new ArrayList<>(before.entrySet());
    }
    public List<Map.Entry<String, Object>> getListAfter() {
        if (null == after || after.isEmpty()) {
            return new ArrayList<>(0);
        }
        return new ArrayList<>(after.entrySet());
    }

    @ToString
    @Data
    public static class Source {
        private String table;
        private String db;
        private String snapshot;
        private Long pos;
        private Long row;
        private Long thread;
        private String query;
        // query

    }


    public static class OperationEnumSerializer implements ObjectSerializer {
        @Override
        public void write(JSONSerializer serializer, Object object, Object fieldName, Type fieldType, int features) throws IOException {
            Envelope.Operation gender = (Envelope.Operation) object;
            serializer.out.writeString(gender.code());
        }
    }

    public static class OperationEnumDeserializer implements ObjectDeserializer {
        @Override
        public <T> T deserialze(DefaultJSONParser parser, Type type, Object fieldName) {
            String value = parser.parseObject(String.class);
            for (Envelope.Operation op : Envelope.Operation.values()) {
                if (op.code().equals(value)) {
                    return (T) op;
                }
            }
            // 没有匹配到，可以抛出异常或者返回null
            return null;
        }

        @Override
        public int getFastMatchToken() {
            return JSONToken.LITERAL_STRING;
        }
    }
}
