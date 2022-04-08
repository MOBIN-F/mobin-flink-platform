package com.mobin.flink.formats.fastJson;

import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.formats.common.TimestampFormat;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import javax.annotation.Nullable;
import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Objects;

import static java.lang.String.format;

/**
 * Created with IDEA
 * Creater: MOBIN
 * Date: 2022/4/8
 * Time: 2:29 下午
 */
public class FastJsonDeserializationSchema implements DeserializationSchema<RowData> {

    private static final long serialVersionUID = 1L;

    /**
     * Flag indicating whether to ignore invalid fields/rows (default: throw an exception).
     */
    private final boolean ignoreParseErrors;
    private final boolean failOnMissingField;

    private final boolean encrypt;

    private final String secretKey;

    /**
     * TypeInformation of the produced {@link RowData}.
     */
    private final TypeInformation<RowData> resultTypeInfo;

    private static final String ALGORITHM = "AES";

    private static final Charset CHARSET = StandardCharsets.UTF_8;

    private final FastJsonToRowDataConverters.FastJsonToRowDataConverter runtimeConverter;

    /**
     * Number of fields.
     */
    private final int fieldCount;

    public FastJsonDeserializationSchema(
            RowType rowType,
            TypeInformation<RowData> resultTypeInfo,
            boolean ignoreParseErrors,
            boolean failOnMissingField,
            boolean encrypt,
            String secretKey,
            TimestampFormat timestampFormat) {
        this.ignoreParseErrors = ignoreParseErrors;
        this.failOnMissingField = failOnMissingField;
        this.encrypt = encrypt;
        this.secretKey = secretKey;
        this.resultTypeInfo = resultTypeInfo;
        this.fieldCount = rowType.getFieldCount();
        this.runtimeConverter =
                new FastJsonToRowDataConverters(ignoreParseErrors, failOnMissingField,timestampFormat)
                        .createConverter(checkNotNull(rowType));
    }

    @Override
    public RowData deserialize(byte[] message) throws IOException {
        if (message == null) {
            return null;
        }
        try {
            return convertToRowData(deserializeToJsonNode(message));
        } catch (Exception t) {
            if (ignoreParseErrors) {
                return null;
            }
            throw new IOException(
                    format("Failed to deserialize JSON '%s'.", new String(message)), t);
        }
    }

    public RowData convertToRowData(Object message) {
        return (RowData) runtimeConverter.convert(message);
    }


    public Object deserializeToJsonNode(byte[] message) throws IOException {
        try {
            if (encrypt && secretKey != null) {
                String content = new String(message, CHARSET);
                byte[] decodedSecretKey = Base64.getDecoder().decode(secretKey);
                SecretKey key = new SecretKeySpec(decodedSecretKey, ALGORITHM);
                Cipher cipher = Cipher.getInstance(ALGORITHM);
                cipher.init(Cipher.DECRYPT_MODE, key);
                byte[] byteContent = Base64.getDecoder().decode(content);
                byte[] byteDecode = cipher.doFinal(byteContent);
                String s = new String(byteDecode);
                Object parse = JSON.parse(s);
//                System.out.println(parse.toString());
                return parse;
            } else {
                String s = new String(message);
                Object parse = JSON.parse(s);
//                System.out.println(parse.toString());
                return parse;
            }
        } catch (Throwable t) {
            if (ignoreParseErrors) {
                return null;
            }
            throw new IOException(format("Failed to deserialize JSON '%s'.", new String(message)), t);
        }
    }

    @Override
    public boolean isEndOfStream(RowData rowData) {
        return false;
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return resultTypeInfo;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        FastJsonDeserializationSchema that = (FastJsonDeserializationSchema) o;
        return ignoreParseErrors == that.ignoreParseErrors
                && failOnMissingField == that.failOnMissingField
                && fieldCount == that.fieldCount
                && encrypt == that.encrypt
                && secretKey == that.secretKey
                && resultTypeInfo.equals(that.resultTypeInfo)
                && Objects.equals(resultTypeInfo, that.resultTypeInfo);
    }

    @Override
    public int hashCode() {
        return Objects.hash(resultTypeInfo, ignoreParseErrors, failOnMissingField,encrypt, secretKey, fieldCount);
    }

    public static <T> T checkNotNull(@Nullable T reference) {
        if (reference == null) {
            throw new NullPointerException();
        }
        return reference;
    }
}

