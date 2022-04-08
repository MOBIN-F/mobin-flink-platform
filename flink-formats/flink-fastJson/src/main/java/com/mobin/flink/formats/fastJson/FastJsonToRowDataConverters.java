package com.mobin.flink.formats.fastJson;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.formats.common.TimestampFormat;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.data.*;
import org.apache.flink.table.types.logical.*;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;
import org.apache.flink.table.types.logical.utils.LogicalTypeUtils;

import java.io.Serializable;
import java.lang.reflect.Array;
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalQueries;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE;
import static org.apache.flink.formats.common.TimeFormats.*;

public class FastJsonToRowDataConverters implements Serializable {
    private static final long serialVersionUID = 1L;

    /**
     * Flag indicating whether to ignore invalid fields/rows (default: throw an exception).
     */
    private final boolean ignoreParseErrors;
    private final boolean failOnMissingField;

    /**
     * Timestamp format specification which is used to parse timestamp.
     */
    private final TimestampFormat timestampFormat;

    public FastJsonToRowDataConverters(boolean ignoreParseErrors, boolean failOnMissingField, TimestampFormat timestampFormat) {
        this.ignoreParseErrors = ignoreParseErrors;
        this.failOnMissingField = failOnMissingField;
        this.timestampFormat = timestampFormat;
    }

    @FunctionalInterface
    public interface FastJsonToRowDataConverter extends Serializable {
        Object convert(Object fastJsonObject);  //需要使用Object，fastjosn包含JSONObject、JSONArray
    }

    public FastJsonToRowDataConverter createConverter(LogicalType type) {
        return wrapIntoNullableConverter(createNotNullConverter(type));
    }

    private FastJsonToRowDataConverter createNotNullConverter(LogicalType type) {
        switch (type.getTypeRoot()) {
            case NULL:
                return jsonObject -> null;
            case BOOLEAN:
                return this::convertToBoolean;
            case TINYINT:
                return fastJsonObject -> Byte.parseByte(fastJsonObject.toString());
            case SMALLINT:
                return fastJsonObject -> Short.parseShort(fastJsonObject.toString());
            case INTEGER:
            case INTERVAL_YEAR_MONTH:
                return this::convertToInt;
            case BIGINT:
            case INTERVAL_DAY_TIME:
                return this::convertToLong;
            case DATE:
                return this::convertToDate;
            case TIME_WITHOUT_TIME_ZONE:
                return this::convertToTime;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return this::convertToTimestamp;
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return this::convertToTimestampWithLocalZone;
            case FLOAT:
                return this::convertToFloat;
            case DOUBLE:
                return this::convertToDouble;
            case CHAR:
            case VARCHAR:
                return this::convertToString;
            case BINARY:
            case VARBINARY:
                return this::convertToBytes;
            case DECIMAL:
                return createDecimalConverter((DecimalType) type);
            case ROW:
                return createRowConverter((RowType) type);
            case ARRAY:
                return createArrayConverter((ArrayType) type);
            case MAP:
                MapType mapType = (MapType) type;
                return createMapConverter(
                        mapType.asSummaryString(), mapType.getKeyType(), mapType.getValueType());
            case MULTISET:
                MultisetType multisetType = (MultisetType) type;
                return createMapConverter(
                        multisetType.asSummaryString(),
                        multisetType.getElementType(),
                        new IntType());
            case RAW:
            default:
                throw new UnsupportedOperationException("Unsupported type: " + type);
        }
    }

    private FastJsonToRowDataConverter createMapConverter(String typeSummary, LogicalType keyType, LogicalType valueType) {
        if (!LogicalTypeChecks.hasFamily(keyType, LogicalTypeFamily.CHARACTER_STRING)) {
            throw new UnsupportedOperationException(
                    "JSON format doesn't support non-string as key type of map. "
                            + "The type is: "
                            + typeSummary);
        }
        final FastJsonToRowDataConverter keyConverter = createConverter(keyType);
        final FastJsonToRowDataConverter valueConverter = createConverter(valueType);

        return fastJsonObject -> {
            JSONObject jsonObject = (JSONObject) fastJsonObject;
            Iterator<Map.Entry<String, Object>> fields = jsonObject.entrySet().iterator();
            Map<Object, Object> result = new HashMap<>();
            while (fields.hasNext()) {
                Map.Entry<String, Object> entry = fields.next();
                Object key = keyConverter.convert(entry.getKey());
                Object value = valueConverter.convert(entry.getValue());
                result.put(key, value);
            }
            return new GenericMapData(result);
        };
    }

    private FastJsonToRowDataConverter createDecimalConverter(DecimalType decimalType) {
        final int precision = decimalType.getPrecision();
        final int scale = decimalType.getScale();
        return fastJsonObject -> {
            BigDecimal bigDecimal = new BigDecimal(fastJsonObject.toString());
            return DecimalData.fromBigDecimal(bigDecimal, precision, scale);
        };
    }

    private byte[] convertToBytes(Object fastJsonObject) {
        return fastJsonObject.toString().getBytes(StandardCharsets.UTF_8);
    }

    private double convertToDouble(Object fastJsonObject) {
        return Double.parseDouble(fastJsonObject.toString());
    }

    private float convertToFloat(Object fastJsonObject) {
        return Float.parseFloat(fastJsonObject.toString());
    }

    private TimestampData convertToTimestampWithLocalZone(Object fastJsonObject) {
        TemporalAccessor parsedTimestampWithLocalZone;
        switch (timestampFormat) {
            case SQL:
                parsedTimestampWithLocalZone =
                        SQL_TIMESTAMP_WITH_LOCAL_TIMEZONE_FORMAT.parse(fastJsonObject.toString());
                break;
            case ISO_8601:
                parsedTimestampWithLocalZone =
                        ISO8601_TIMESTAMP_WITH_LOCAL_TIMEZONE_FORMAT.parse(fastJsonObject.toString());
                break;
            default:
                throw new TableException(
                        String.format(
                                "Unsupported timestamp format '%s'. Validator should have checked that.",
                                timestampFormat));
        }
        LocalTime localTime = parsedTimestampWithLocalZone.query(TemporalQueries.localTime());
        LocalDate localDate = parsedTimestampWithLocalZone.query(TemporalQueries.localDate());

        return TimestampData.fromInstant(
                LocalDateTime.of(localDate, localTime).toInstant(ZoneOffset.UTC));
    }

    private TimestampData convertToTimestamp(Object fastJsonObject) {
        TemporalAccessor parsedTimestamp;
        switch (timestampFormat) {
            case SQL:
                parsedTimestamp = SQL_TIMESTAMP_FORMAT.parse(fastJsonObject.toString());
                break;
            case ISO_8601:
                parsedTimestamp = ISO8601_TIMESTAMP_FORMAT.parse(fastJsonObject.toString());
                break;
            default:
                throw new TableException(
                        String.format(
                                "Unsupported timestamp format '%s'. Validator should have checked that.",
                                timestampFormat));
        }
        LocalTime localTime = parsedTimestamp.query(TemporalQueries.localTime());
        LocalDate localDate = parsedTimestamp.query(TemporalQueries.localDate());

        return TimestampData.fromLocalDateTime(LocalDateTime.of(localDate, localTime));
    }

    private int convertToTime(Object fastJsonObject) {
        TemporalAccessor parsedTime = SQL_TIME_FORMAT.parse(fastJsonObject.toString());
        LocalTime localTime = parsedTime.query(TemporalQueries.localTime());

        // get number of milliseconds of the day
        return localTime.toSecondOfDay() * 1000;
    }

    private int convertToDate(Object fastJsonObject) {
        LocalDate date = ISO_LOCAL_DATE.parse(fastJsonObject.toString()).query(TemporalQueries.localDate());
        return (int) date.toEpochDay();
    }

    private long convertToLong(Object fastJsonObject) {
        return Long.parseLong(fastJsonObject.toString());
    }

    private boolean convertToBoolean(Object fastJsonObject) {
        return Boolean.parseBoolean(fastJsonObject.toString());
    }

    private int convertToInt(Object fastJsonObject) {
        return Integer.parseInt(fastJsonObject.toString());
    }

    private FastJsonToRowDataConverter createArrayConverter(ArrayType arrayType) {
        FastJsonToRowDataConverter elementConverter = createConverter(arrayType.getElementType());
        final Class<?> elementClass =
                LogicalTypeUtils.toInternalConversionClass(arrayType.getElementType());
        return fastJsonObject -> {
            JSONArray jsonArray = JSON.parseArray(fastJsonObject.toString());
            JSONArray tmp = new JSONArray();
            for (int i = 0; i < jsonArray.size(); i++) {
                if (jsonArray.get(i) != null) {
                    tmp.add(jsonArray.get(i));
                }
            }
            jsonArray = tmp;
            final Object[] array = (Object[]) Array.newInstance(elementClass, jsonArray.size());
            for (int i = 0; i < jsonArray.size(); i++) {
                try {
                    if (elementClass.getName().equals("org.apache.flink.table.data.StringData")) {
                        Object object = jsonArray.getString(i);
                        Object convert = elementConverter.convert(object);
                        array[i] = convert;
                    } else if (!elementClass.getName().equals("org.apache.flink.table.data.RowData")) {
                        Object object = jsonArray.getObject(i, (Type) Class.forName(elementClass.getName()));
                        Object convert = elementConverter.convert(object);
                        array[i] = convert;
                    } else {
                        JSONObject innerJson = jsonArray.getJSONObject(i);
                        Object convert = elementConverter.convert(innerJson);
                        if (convert != null) {
                            array[i] = convert;
                        } else {
                            array[i] = "";
                        }
                    }
                } catch (ClassNotFoundException e) {
                    e.printStackTrace();
                }
            }
            return new GenericArrayData(array);
        };
    }

    private FastJsonToRowDataConverter createRowConverter(RowType rowType) {
        final FastJsonToRowDataConverter[] fieldConverters =
                rowType.getFields().stream()
                        .map(RowType.RowField::getType)
                        .map(x -> createConverter(x))
                        .toArray(FastJsonToRowDataConverter[]::new);
        final String[] fieldNames = rowType.getFieldNames().toArray(new String[0]);

        return fastJsonObject -> {
            JSONObject node = (JSONObject) fastJsonObject;
            int arity = fieldNames.length;
            GenericRowData row = new GenericRowData(arity);
            for (int i = 0; i < arity; i++) {
                String fieldName = fieldNames[i];
                String fieldOrJson = node.getString(fieldName);
                Object convertedField = convertField(fieldConverters[i], fieldName, fieldOrJson);
                row.setField(i, convertedField);
            }
            return row;
        };
    }

    private Object convertField(
            FastJsonToRowDataConverter fieldConverter, String fieldName, Object fieldOrJson) {
        if (fieldOrJson == null) {
            if (failOnMissingField) {
                throw new JsonParseException("Could not find field with name '" + fieldName + "'.");
            } else {
                return null;
            }
        } else if (JSON.isValidObject(fieldOrJson.toString())) {  //判断是否json
            JSONObject jsonObject = JSON.parseObject(fieldOrJson.toString());
            return fieldConverter.convert(jsonObject);
        } else if (JSON.isValidArray(fieldOrJson.toString())) {
            JSONArray jsonArray = JSON.parseArray(fieldOrJson.toString());
            return fieldConverter.convert(jsonArray);
        } else {
            return fieldConverter.convert(fieldOrJson);
        }
    }

    private StringData convertToString(Object fastJsonObject) {
        return StringData.fromString(fastJsonObject.toString());
    }

    private FastJsonToRowDataConverter wrapIntoNullableConverter(FastJsonToRowDataConverter converter) {
        return fastJsonObject -> {
            if (fastJsonObject == null) {
                return null;
            }
            try {
                return converter.convert(fastJsonObject);
            } catch (Throwable t) {
                if (!ignoreParseErrors) {
                    throw t;
                }
                return null;
            }
        };
    }

    /**
     * Exception which refers to parse errors in converters.
     */
    private static final class JsonParseException extends RuntimeException {
        private static final long serialVersionUID = 1L;

        public JsonParseException(String message) {
            super(message);
        }
    }
}