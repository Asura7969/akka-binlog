package com.asura.bigdata.helper;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.HashMap;
import java.util.Map;

/**
 * @author asura7969
 * @create 2022-07-14-21:18
 */
@Data
public class TableSchema {

    private Map<String, JdbcHelper.SqlType> columns;

    public TableSchema() {
        columns = new HashMap<>();
    }

    public TableSchema addField(String fieldName, JdbcHelper.SqlType type) {
        columns.put(fieldName, type);
        return this;
    }
}
