package com.asura.bigdata.model;

import io.debezium.data.Envelope;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.List;

@Data
@AllArgsConstructor
public class SqlOperatorRow {
    private Envelope.Operation op;
    private String sql;
    private List<Object> values;

    public static SqlOperatorRow empty() {
        return new SqlOperatorRow(null, null, null);
    }

    public boolean isEmpty() {
        return null == op;
    }
}
