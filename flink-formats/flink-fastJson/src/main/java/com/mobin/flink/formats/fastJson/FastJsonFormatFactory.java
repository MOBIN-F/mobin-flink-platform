package com.mobin.flink.formats.fastJson;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.formats.common.TimestampFormat;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.SerializationFormatFactory;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class FastJsonFormatFactory implements DeserializationFormatFactory, SerializationFormatFactory {

    public static final String IDENTIFIER = "fc-json";

    public static final ConfigOption<Boolean> IGNORE_PARSE_ERRORS = FastJsonOptions.IGNORE_PARSE_ERRORS;
    public static final ConfigOption<Boolean> FAIL_ON_MISSING_FIELD = FastJsonOptions.FAIL_ON_MISSING_FIELD;
    public static final ConfigOption<String> TIMESTAMP_FORMAT = FastJsonOptions.TIMESTAMP_FORMAT;

    @Override
    public DecodingFormat<DeserializationSchema<RowData>> createDecodingFormat(DynamicTableFactory.Context context, ReadableConfig readableConfig) {
        FactoryUtil.validateFactoryOptions(this,readableConfig);
        final boolean ignoreParseErrors = readableConfig.get(IGNORE_PARSE_ERRORS);
        final boolean failOnMissingField = readableConfig.get(FAIL_ON_MISSING_FIELD);
        TimestampFormat timestampFormat = FastJsonOptions.getTimestampFormat(readableConfig);
        return new DecodingFormat<DeserializationSchema<RowData>>() {
            @Override
            public DeserializationSchema<RowData> createRuntimeDecoder(DynamicTableSource.Context context, DataType dataType) {
                final RowType rowType = (RowType) dataType.getLogicalType();
                final TypeInformation<RowData> rowDataTypeInformation = context.createTypeInformation(dataType);
                return new FastJsonDeserializationSchema(
                        rowType,
                        rowDataTypeInformation,
                        ignoreParseErrors,
                        failOnMissingField,
                        timestampFormat);
            }

            @Override
            public ChangelogMode getChangelogMode() {
                return ChangelogMode.newBuilder()
                        .addContainedKind(RowKind.INSERT)
                        .build();
            }
        };
    }

    @Override
    public EncodingFormat<SerializationSchema<RowData>> createEncodingFormat(DynamicTableFactory.Context context, ReadableConfig readableConfig) {
        throw new UnsupportedOperationException("fc-json format doesn't support as a sink format yet. Please use 'format='json''");
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return Collections.emptySet();
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(IGNORE_PARSE_ERRORS);
        options.add(FAIL_ON_MISSING_FIELD);
        options.add(TIMESTAMP_FORMAT);
        return options;
    }
}
