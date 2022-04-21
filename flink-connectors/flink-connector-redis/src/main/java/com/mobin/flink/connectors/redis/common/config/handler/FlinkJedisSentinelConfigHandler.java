package com.mobin.flink.connectors.redis.common.config.handler;


import com.mobin.flink.connectors.redis.common.config.FlinkJedisConfigBase;
import com.mobin.flink.connectors.redis.common.config.FlinkJedisSentinelConfig;
import com.mobin.flink.connectors.redis.common.handler.FlinkJedisConfigHandler;
import com.mobin.flink.connectors.redis.descriptor.RedisValidator;

import java.util.*;

public class FlinkJedisSentinelConfigHandler implements FlinkJedisConfigHandler {

    @Override
    public FlinkJedisConfigBase createFlinkJedisConfig(Map<String, String> properties) {
        String masterName = properties.computeIfAbsent(RedisValidator.REDIS_MASTER_NAME, null);
        String sentinelsInfo = properties.computeIfAbsent(RedisValidator.SENTINELS_INFO, null);
        Objects.requireNonNull(masterName, "master should not be null in sentinel mode");
        Objects.requireNonNull(sentinelsInfo, "sentinels should not be null in sentinel mode");
        Set<String> sentinels = new HashSet<>(Arrays.asList(sentinelsInfo.split(",")));
        String sentinelsPassword = properties.computeIfAbsent(RedisValidator.SENTINELS_PASSWORD, null);
        if (sentinelsPassword != null && sentinelsPassword.trim().isEmpty()) {
            sentinelsPassword = null;
        }
        FlinkJedisSentinelConfig flinkJedisSentinelConfig = new FlinkJedisSentinelConfig.Builder()
                .setMasterName(masterName).setSentinels(sentinels).setPassword(sentinelsPassword)
                .build();
        return flinkJedisSentinelConfig;
    }

    @Override
    public Map<String, String> requiredContext() {
        Map<String, String> require = new HashMap<>();
        require.put(RedisValidator.REDIS_MODE, RedisValidator.REDIS_SENTINEL);
        return require;
    }

    public FlinkJedisSentinelConfigHandler() {

    }
}
