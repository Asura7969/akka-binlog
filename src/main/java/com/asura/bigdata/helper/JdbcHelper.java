package com.asura.bigdata.helper;

import lombok.extern.slf4j.Slf4j;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static com.asura.bigdata.helper.JdbcHelper.SqlType.*;

/**
 * @author asura7969
 * @create 2022-07-14-21:04
 */
@Slf4j
public class JdbcHelper {


    public static enum SqlType {
        VARCHAR, BOOLEAN, TINYINT, SMALLINT, INTEGER, BIGINT, REAL, DOUBLE,
        DATE, TIME, TIMESTAMP, DECIMAL, BINARY, OBJECT_ARRAY, PRIMITIVE_ARRAY
    }

    private static final Map<SqlType, Integer> TYPE_MAPPING;

    static {
        HashMap<SqlType, Integer> m = new HashMap<>();
        m.put(VARCHAR, Types.VARCHAR);
        m.put(BOOLEAN, Types.BOOLEAN);
        m.put(TINYINT, Types.TINYINT);
        m.put(SMALLINT, Types.SMALLINT);
        m.put(INTEGER, Types.INTEGER);
        m.put(BIGINT, Types.BIGINT);
        m.put(REAL, Types.REAL);
        m.put(DOUBLE, Types.DOUBLE);
        m.put(DATE, Types.DATE);
        m.put(TIME, Types.TIME);
        m.put(TIMESTAMP, Types.TIMESTAMP);
        m.put(DECIMAL, Types.DECIMAL);
        m.put(BINARY, Types.BINARY);
        TYPE_MAPPING = Collections.unmodifiableMap(m);
    }

    public static int sqlTypeToRealType(SqlType type) {

        if (TYPE_MAPPING.containsKey(type)) {
            return TYPE_MAPPING.get(type);
        } else if (type.equals(OBJECT_ARRAY) || type.equals(PRIMITIVE_ARRAY)) {
            return Types.ARRAY;
        } else {
            throw new IllegalArgumentException("Unsupported type: " + type);
        }
    }

    public static void setField(PreparedStatement upload, int type, Object field, int index)
            throws SQLException {
        if (field == null) {
            upload.setNull(index + 1, type);
        } else {
            try {
                // casting values as suggested by
                // http://docs.oracle.com/javase/1.5.0/docs/guide/jdbc/getstart/mapping.html
                switch (type) {
                    case java.sql.Types.NULL:
                        upload.setNull(index + 1, type);
                        break;
                    case java.sql.Types.BOOLEAN:
                    case java.sql.Types.BIT:
                        upload.setBoolean(index + 1, (boolean) field);
                        break;
                    case java.sql.Types.CHAR:
                    case java.sql.Types.NCHAR:
                    case java.sql.Types.VARCHAR:
                    case java.sql.Types.LONGVARCHAR:
                    case java.sql.Types.LONGNVARCHAR:
                        upload.setString(index + 1, (String) field);
                        break;
                    case java.sql.Types.TINYINT:
                        upload.setByte(index + 1, (byte) field);
                        break;
                    case java.sql.Types.SMALLINT:
                        upload.setShort(index + 1, (short) field);
                        break;
                    case java.sql.Types.INTEGER:
                        upload.setInt(index + 1, (int) field);
                        break;
                    case java.sql.Types.BIGINT:
                        upload.setLong(index + 1, (long) field);
                        break;
                    case java.sql.Types.REAL:
                        upload.setFloat(index + 1, (float) field);
                        break;
                    case java.sql.Types.FLOAT:
                    case java.sql.Types.DOUBLE:
                        upload.setDouble(index + 1, (double) field);
                        break;
                    case java.sql.Types.DECIMAL:
                    case java.sql.Types.NUMERIC:
                        upload.setBigDecimal(index + 1, (java.math.BigDecimal) field);
                        break;
                    case java.sql.Types.DATE:
                        upload.setDate(index + 1, (java.sql.Date) field);
                        break;
                    case java.sql.Types.TIME:
                        upload.setTime(index + 1, (java.sql.Time) field);
                        break;
                    case java.sql.Types.TIMESTAMP:
                        upload.setTimestamp(index + 1, (java.sql.Timestamp) field);
                        break;
                    case java.sql.Types.BINARY:
                    case java.sql.Types.VARBINARY:
                    case java.sql.Types.LONGVARBINARY:
                        upload.setBytes(index + 1, (byte[]) field);
                        break;
                    default:
                        upload.setObject(index + 1, field);
                        log.warn(
                                "Unmanaged sql type ({}) for column {}. Best effort approach to set its value: {}.",
                                type,
                                index + 1,
                                field);
                        // case java.sql.Types.SQLXML
                        // case java.sql.Types.ARRAY:
                        // case java.sql.Types.JAVA_OBJECT:
                        // case java.sql.Types.BLOB:
                        // case java.sql.Types.CLOB:
                        // case java.sql.Types.NCLOB:
                        // case java.sql.Types.DATALINK:
                        // case java.sql.Types.DISTINCT:
                        // case java.sql.Types.OTHER:
                        // case java.sql.Types.REF:
                        // case java.sql.Types.ROWID:
                        // case java.sql.Types.STRUC
                }
            } catch (ClassCastException e) {
                // enrich the exception with detailed information.
                String errorMessage =
                        String.format(
                                "%s, field index: %s, field value: %s.",
                                e.getMessage(), index, field);
                ClassCastException enrichedException = new ClassCastException(errorMessage);
                enrichedException.setStackTrace(e.getStackTrace());
                throw enrichedException;
            }
        }
    }




}
