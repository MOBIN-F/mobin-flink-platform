package com.mobin.flink.connectors.redis;

import com.mobin.flink.connectors.redis.common.Util;
import com.mobin.flink.connectors.redis.common.container.RedisCommandsContainer;
import com.mobin.flink.connectors.redis.common.container.RedisCommandsContainerBuilder;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.shaded.guava18.com.google.common.cache.Cache;
import org.apache.flink.shaded.guava18.com.google.common.cache.CacheBuilder;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.connector.source.TableFunctionProvider;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static com.mobin.flink.connectors.redis.RedisDynamicTableFactory.*;


/**
 * Created with IDEA
 * Creater: MOBIN
 * Date: 2021/6/14
 * Time: 11:35 PM
 */
public class RedisDynamicTableSource implements LookupTableSource{
    private static final Logger LOG = LoggerFactory.getLogger(RedisDynamicTableSource.class);
    private final ReadableConfig config;
    private final TableSchema schema;

    public RedisDynamicTableSource(ReadableConfig config, TableSchema schema) {
        this.config = config;
        this.schema = schema;
    }


    @Override
    public LookupRuntimeProvider getLookupRuntimeProvider(LookupContext lookupContext) {
        Preconditions.checkArgument(lookupContext.getKeys().length == 1
        && lookupContext.getKeys()[0].length == 1,
                "Redis source only supports lookup by single key");

        int fieldCount = schema.getFieldCount();
        if (fieldCount != 2) {
            throw new ValidationException("Redis source only support 2 columns");
        }

        DataType[] dataTypes = schema.getFieldDataTypes();
        for (int i = 0; i < fieldCount; i ++) {
            if (!dataTypes[i].getLogicalType().getTypeRoot().equals(LogicalTypeRoot.VARCHAR)) {
                throw new ValidationException("Redis conector only supports STRING type");
            }
        }
        return TableFunctionProvider.of(new RedisRowDataLookupFunction(config, schema.getFieldName(1).get()));
    }

    @Override
    public DynamicTableSource copy() {
        return null;
    }

    @Override
    public String asSummaryString() {
        return null;
    }

    //TODO 改成异步
    public static class RedisRowDataLookupFunction extends TableFunction<RowData> {

        private static final long serialVersionUID = 1L;
        private final ReadableConfig config;
        private final String type;
        private final String AdditionalKey;
        private final int cacheMaxRows;
        private final int cacheTTLSec;
        private final int maxRetry;
        private final String jsonFiled;

        private RedisCommandsContainer commandsContainer;
        private transient Cache<RowData, RowData> cache;

        public RedisRowDataLookupFunction(ReadableConfig config, String jsonFiled) {
            Preconditions.checkNotNull(config, "No config supplied");
            this.config = config;
            this.jsonFiled = jsonFiled;

            type = config.get(COMMAND).toUpperCase();
            Preconditions.checkArgument(type.equals("GET") || type.equals("HGET"), "Redis table  only supports GET and HGET type");

            AdditionalKey = config.get(ADDITIONAL_KEY);
            cacheMaxRows = config.get(LOOKUP_CACHE_MAX_ROWS);
            cacheTTLSec = config.get(LOOKUP_CACHE_TTL_SEC);
            maxRetry = config.get(LOOKUP_MAX_RETRIES);
        }

        @Override
        public void open(FunctionContext context) throws Exception {
            super.open(context);
            commandsContainer = RedisCommandsContainerBuilder.build(Util.getFlinkJedisConfig(config));
            commandsContainer.open();
            this.cache = cacheMaxRows == -1 || cacheTTLSec == -1 ? null: CacheBuilder.newBuilder()
                            .expireAfterWrite(cacheTTLSec, TimeUnit.SECONDS)
                            .maximumSize(cacheMaxRows)
                            .build();
        }

        @Override
        public void close() throws Exception {
            if (cache != null) {
                cache.invalidateAll();
            }
            if (commandsContainer != null) {
                commandsContainer.close();
            }
            super.close();
        }

        public void eval(Object obj) throws IOException {
            RowData rowData = GenericRowData.of(obj);
            if (cache != null) {
                RowData cacheRow = cache.getIfPresent(rowData);
                if (cacheRow != null) {
                    collect(cacheRow);
                    return;
                }
            }
            for (int retry = 1; retry <= maxRetry; retry ++) {
                try {
                    String key = rowData.getString(0).toString();
                    String value;
                    if (config.get(FORMAT) == null || !config.get(FORMAT).equalsIgnoreCase("json")) {
                        value = type.equals("GET") ?
                                commandsContainer.get(key):
                                commandsContainer.hget(AdditionalKey, key);
                    }else {
                        value =  type.equals("GET") ?
                                Util.parseJson(jsonFiled,commandsContainer.get(key)):
                                Util.parseJson(jsonFiled,commandsContainer.hget(AdditionalKey, key));
                    }

                    RowData result = GenericRowData.of(rowData.getString(0), StringData.fromString(value));
                    collect(result);
                    if (cache != null) {
                        cache.put(rowData, result);
                    }
                    break;
                }catch (Exception e) {
                    LOG.error(String.format("Read get data error, retry times =%d", retry), e);
                    if (retry >= maxRetry) {
                        throw new RuntimeException("Redis commandsContainer failed.", e);
                    }
                    try {
                        Thread.sleep(1000 * retry);
                    } catch (InterruptedException e1) {
                        throw new RuntimeException(e1);
                    }
                }
            }

        }
    }
}
