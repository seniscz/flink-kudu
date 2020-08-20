package com.birdsoaring.utils;

import org.apache.commons.math3.util.Decimal64;
import org.apache.kudu.Type;
import org.apache.kudu.client.PartialRow;
import org.apache.kudu.client.RowResult;
import org.apache.kudu.shaded.com.google.common.collect.ImmutableBiMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Date;

/**
 * @ClassName TableUtils
 * @Description TODO
 * @Author chezhao
 * @Date 2020/4/15 10:40
 * @Version 1.0
 **/
public class TableUtils {
    private static final Logger logger = LoggerFactory.getLogger(TableUtils.class);

    private final static ImmutableBiMap<Type, Class> TYPES =
            new ImmutableBiMap.Builder<Type, Class>()
                    .put(Type.STRING, String.class)
                    .put(Type.BOOL, Boolean.class)
                    .put(Type.DOUBLE, Double.class)
                    .put(Type.FLOAT, Float.class)
                    .put(Type.DECIMAL, Decimal64.class)
                    .put(Type.BINARY, ByteBuffer.class)
                    .put(Type.INT8, Byte.class)
                    .put(Type.INT16, Short.class)
                    .put(Type.INT32, Integer.class)
                    .put(Type.INT64, Long.class)
                    .put(Type.UNIXTIME_MICROS, Date.class)
                    .build();


    public final static Type mapToType(Class clazz) {
        return TYPES.inverse().get(clazz);
    }

    public final static Class mapFromType(Type type) {
        return TYPES.get(type);
    }

    /**
     * get row
     *
     * @param row represents one row from a scanner
     * @param col column type
     * @return column value
     */
    public final static Object valueFromRow(RowResult row, String col) {
        Object value = new Object();
        Type colType = row.getColumnType(col);
        switch (colType) {
            case STRING:
                value = row.getString(col);
                break;
            case BOOL:
                value = row.getBoolean(col);
                break;
            case DOUBLE:
                value = row.getDouble(col);
                break;
            case FLOAT:
                value = row.getFloat(col);
                break;
            case DECIMAL:
                value = row.getDecimal(col);
            case BINARY:
                value = row.getBinary(col);
                break;
            case INT8:
                value = row.getByte(col);
                break;
            case INT16:
                value = row.getShort(col);
                break;
            case INT32:
                value = row.getInt(col);
                break;
            case INT64:
                value = row.getLong(col);
                break;
            case UNIXTIME_MICROS:
                value = new Date(row.getLong(col) / 1000);
                break;
            default:
                logger.error("No matching field type");
                break;
        }
        return value;
    }

    private final static <T> T mapValue(Object value, Class clazz) {
        return (T) value;
    }

    /**
     * insert row
     *
     * @param row
     * @param colType
     * @param col
     * @param value
     */
    public final static void valueToRow(PartialRow row, Type colType, String col, Object value) {
        switch (colType) {
            case STRING:
                row.addString(col, mapValue(value, mapFromType(colType)));
                break;
            case BOOL:
                row.addBoolean(col, mapValue(value, mapFromType(colType)));
                break;
            case DOUBLE:
                row.addDouble(col, mapValue(value, mapFromType(colType)));
                break;
            case FLOAT:
                row.addFloat(col, mapValue(value, mapFromType(colType)));
                break;
            case DECIMAL:
                row.addDecimal(col, mapValue(value, mapFromType(colType)));
            case BINARY:
                //row.addBinary(col, mapValue(value, mapFromType(colType)));
                break;
            case INT8:
                row.addByte(col, mapValue(value, mapFromType(colType)));
                break;
            case INT16:
                row.addShort(col, mapValue(value, mapFromType(colType)));
                break;
            case INT32:
                row.addInt(col, mapValue(value, mapFromType(colType)));
                break;
            case INT64:
                row.addLong(col, mapValue(value, mapFromType(colType)));
                break;
            case UNIXTIME_MICROS:
                row.addLong(col, mapValue(value, mapFromType(colType)));
                break;
            default:
                logger.error("No matching field type");
                break;
        }
    }
}
