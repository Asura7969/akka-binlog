package com.asura.bigdata.helper;

import com.asura.bigdata.model.SqlOperatorRow;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static io.debezium.data.Envelope.Operation.*;

public class EventHelper {

    public static SqlOperatorRow handleUpdate(String table,
                                               Map<String, Object> before,
                                               Map<String, Object> after) {
        List<Object> newVList = new ArrayList<>();
        List<String> sb = new ArrayList();
        before.forEach((column, beforeV) -> {
            Object newValue = after.get(column);
            if (null == beforeV && null != newValue) {
                sb.add(column + "=?");
                newVList.add(newValue);
            } else if (null != beforeV && null == newValue) {
                sb.add(column + "=?");
                newVList.add(null);
            } else if (!beforeV.equals(newValue)) {
                sb.add(column + "=?");
                newVList.add(newValue);
            }
        });

        String sql = String.format("update %s where %s", table, String.join(",", sb));
        return new SqlOperatorRow(UPDATE, sql, newVList);
    }

    public static SqlOperatorRow handleDelete(String table,
                                               List<Map.Entry<String, Object>> before) {
        String condition = before.stream().map(entry -> entry.getKey() + "=?").collect(Collectors.joining(","));
        String sql = String.format("delete from %s where %s", table, condition);
        return new SqlOperatorRow(DELETE, sql, before.stream().map(Map.Entry::getValue).collect(Collectors.toList()));
    }

    public static SqlOperatorRow handleInsert(String table,
                                               List<Map.Entry<String, Object>> after) {
        String columns = after.stream().map(Map.Entry::getKey).collect(Collectors.joining(","));
        String placeholder = after.stream().map((ignore) -> "?").collect(Collectors.joining(","));
        String sql = String.format("insert into %s(%s) values(%s)", table, columns, placeholder);
        return new SqlOperatorRow(CREATE, sql, after.stream().map(Map.Entry::getValue).collect(Collectors.toList()));
    }
}
