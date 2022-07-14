package com.asura.bigdata.debezium;

import io.debezium.data.Envelope;
import org.apache.kafka.connect.data.Struct;

public class DataEvent {

    private Struct sourceRecordValue;
    private String tableName;
    private Envelope.Operation operation;


    public DataEvent(Struct sourceRecordValue, String tableName, Envelope.Operation operation) {
        this.sourceRecordValue = sourceRecordValue;
        this.tableName = tableName;
        this.operation = operation;
    }

    @Override
    public String toString() {
        return "DataEvent{" +
                "sourceRecordValue=" + sourceRecordValue +
                ", tableName='" + tableName + '\'' +
                ", operation=" + operation +
                '}';
    }
}
