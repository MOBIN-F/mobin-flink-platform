package com.mobin.flink.connectors.redis;

import com.mobin.flink.connectors.redis.common.Util;
import com.mobin.flink.connectors.redis.common.mapper.RedisCommand;
import com.mobin.flink.connectors.redis.common.mapper.RedisCommandDescription;
import com.mobin.flink.connectors.redis.common.mapper.RedisDataType;
import com.mobin.flink.connectors.redis.common.mapper.RedisMapper;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.RowKind;

import java.util.Optional;


/**
 * Created with IDEA
 * Creater: MOBIN
 * Date: 2021/6/1
 * Time: 11:52 PM
 */
public class RedisDynamicTableSink implements DynamicTableSink {

    private final ReadableConfig config;
    private final TableSchema schema;

    public RedisDynamicTableSink(ReadableConfig config, TableSchema schema) {
        this.config = config;
        this.schema = schema;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode changelogMode) {
        return ChangelogMode.newBuilder()
                .addContainedKind(RowKind.INSERT)
                .addContainedKind(RowKind.UPDATE_AFTER)
                .addContainedKind(RowKind.UPDATE_BEFORE)
                .build();
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        RedisCommand command = RedisCommand.valueOf(config.get(RedisDynamicTableFactory.COMMAND).toUpperCase());

        int fieldCount = schema.getFieldCount();
        if (fieldCount != (needAdditionalKey(command) ? 3 : 2)) {
            throw new ValidationException("Redis sink only supports 2 or 3 columns");
        }
        RedisMapper<RowData> redisRowData = new RedisRowData(config, command, schema.getFieldDataTypes());
        RedisSink<RowData> rowDataRedisSink = new RedisSink<>(Util.getFlinkJedisConfig(config), redisRowData);
        return SinkFunctionProvider.of(rowDataRedisSink);
    }

    @Override
    public DynamicTableSink copy() {
        return new RedisDynamicTableSink(config, schema);
    }

    @Override
    public String asSummaryString() {
        return "Redis Dynamic Table Sink";
    }

    public static boolean needAdditionalKey(RedisCommand command) {
        return command.getRedisDataType() == RedisDataType.HASH || command.getRedisDataType() == RedisDataType.SORTED_SET;
    }

    public static final class RedisRowData implements RedisMapper<RowData> {
        private static final long serialVersionUID = 1L;

        private final ReadableConfig config;
        private final RedisCommand command;
        private final DataType[] dataTypes;

        public RedisRowData(ReadableConfig config, RedisCommand command, DataType[] dataTypes) {
            this.config = config;
            this.command = command;
            this.dataTypes = dataTypes;
        }

        @Override
        public Optional<String> getAdditionalKey(RowData rowData) {
            return needAdditionalKey(command) ? Optional.of(rowData.getString(0).toString()) : Optional.empty();
        }

        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(command);
        }

        @Override
        public String getKeyFromData(RowData rowData) {
            String key = rowData.getString(needAdditionalKey(command) ? 1 : 0).toString();
            return key.trim();
        }

        @Override
        public String getValueFromData(RowData rowData) {
            return rowData.getString(needAdditionalKey(command) ? 2 : 1).toString();
        }

        @Override
        public Optional<Integer> getAdditionalTTL(RowData data) {
            return config.getOptional(RedisDynamicTableFactory.TTL_SEC);
        }
    }
}
