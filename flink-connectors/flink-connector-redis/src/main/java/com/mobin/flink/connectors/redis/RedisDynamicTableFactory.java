package com.mobin.flink.connectors.redis;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.util.StringUtils;

import java.util.HashSet;
import java.util.Set;

import static org.apache.flink.configuration.ConfigOptions.key;

/**
 * Created with IDEA
 * Creater: MOBIN
 * Date: 2021/6/1
 * Time: 8:54 PM
 */
public class RedisDynamicTableFactory implements DynamicTableSinkFactory, DynamicTableSourceFactory {

    public static final String IDENTIFIER = "redis";

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        helper.validate();
        ReadableConfig config = helper.getOptions();
        validateConfigOptions(config);
        TableSchema schema = context.getCatalogTable().getSchema();
        return new RedisDynamicTableSink(config, schema);
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        helper.validate();
        ReadableConfig config = helper.getOptions();
        validateConfigOptions(config);
        TableSchema schema = context.getCatalogTable().getSchema();
        return new RedisDynamicTableSource(config, schema);
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(MODE);
        options.add(COMMAND);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(HOST);
        options.add(TTL_SEC);
        options.add(ADDITIONAL_KEY);
        options.add(PASSWORD);
        options.add(PORT);
        options.add(LOOKUP_CACHE_MAX_ROWS);
        options.add(LOOKUP_CACHE_TTL_SEC);
        options.add(CONNECTION_TIMEOUT);
        options.add(CONNECTION_MAX_TOTAL);
        options.add(CONNECTION_MAX_IDELE);
        options.add(CONNECTION_MAX_REDIRECTIONS);
        options.add(CONNECTION_MIN_IDELE);
        options.add(LOOKUP_MAX_RETRIES);
        options.add(FORMAT);
        return options;
    }

    public static final ConfigOption<String> PASSWORD = ConfigOptions.key("password")
            .stringType()
            .noDefaultValue()
            .withDescription("Redis Server password");

    public static final ConfigOption<String> HOST = ConfigOptions.key("host")
            .stringType()
            .noDefaultValue()
            .withDescription("Redis Server host");

    public static final ConfigOption<Integer> PORT = ConfigOptions.key("port")
            .intType()
            .defaultValue(6379)
            .withDescription("Redis Server port");

    public static final ConfigOption<String> MODE = ConfigOptions.key("mode")
            .stringType()
            .noDefaultValue()
            .withDescription("Redis Server mode");

    public static final ConfigOption<String> FORMAT = ConfigOptions.key("format")
            .stringType()
            .noDefaultValue()
            .withDescription("Redis value format");


    public static final ConfigOption<String> COMMAND = ConfigOptions
            .key("command")
            .stringType()
            .noDefaultValue()
            .withDescription("Redis command");

    public static final ConfigOption<Integer> TTL_SEC = ConfigOptions
            .key("ttl-sec")
            .intType()
            .noDefaultValue();

    public static final ConfigOption<Integer> CONNECTION_MAX_IDELE = ConfigOptions
            .key("connection.max-idle")
            .intType()
            .defaultValue(15);

    public static final ConfigOption<Integer> CONNECTION_MIN_IDELE = ConfigOptions
            .key("connection.min-idle")
            .intType()
            .defaultValue(3);

    public static final ConfigOption<Integer> CONNECTION_MAX_REDIRECTIONS = ConfigOptions
            .key("connection.max-redirections")
            .intType()
            .defaultValue(10);

    public static final ConfigOption<Integer> CONNECTION_TIMEOUT = ConfigOptions
            .key("connection.timeout")
            .intType()
            .defaultValue(10000);

    public static final ConfigOption<Integer> CONNECTION_MAX_TOTAL = ConfigOptions
            .key("connection.max.total")
            .intType()
            .defaultValue(15);

    public static final ConfigOption<String> ADDITIONAL_KEY = key("additional-key")
            .stringType()
            .noDefaultValue()
            .withDescription("additional-key");

    public static final ConfigOption<Integer> LOOKUP_CACHE_MAX_ROWS = key("lookup.cache.max-rows")
            .intType()
            .defaultValue(-1)
            .withDescription("lookup.cache.max-rows");

    public static final ConfigOption<Integer> LOOKUP_CACHE_TTL_SEC = key("lookup.cache.ttl-sec")
            .intType()
            .defaultValue(-1)
            .withDescription("lookup.cache.ttl-sec");

    public static final ConfigOption<Integer> LOOKUP_MAX_RETRIES = key("lookup.max-retries")
            .intType()
            .defaultValue(3)
            .withDescription("lookup.max-retries");

    private void validateConfigOptions(ReadableConfig config) {
        switch (config.get(MODE)) {
            case "single":
                if (StringUtils.isNullOrWhitespaceOnly(config.getOptional(HOST).get())) {
                    throw new IllegalArgumentException("Parameter host and post must be provided in single mode");
                }
                break;
            case "cluster":
                if (StringUtils.isNullOrWhitespaceOnly(config.getOptional(HOST).get())) {
                    throw new IllegalArgumentException("Parameter host must be provided in cluster mode");
                }
                break;
            default:
                throw new IllegalArgumentException("The node parameter must be provided, supports single and cluster");
        }
        if (config.get(FORMAT)!= null && !config.get(FORMAT).equalsIgnoreCase("json")) {
            throw new IllegalArgumentException("format only support 'json'");
        }

    }

}
