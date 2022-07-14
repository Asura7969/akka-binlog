package com.asura.bigdata.debezium;

import org.apache.kafka.connect.source.SourceRecord;

import java.io.Serializable;

public interface DebeziumDeserializationSchema<T> extends Serializable {
    T deserialize(SourceRecord record) throws Exception;
}
