package com.mobin.flink.connectors.redis.common;

/**
 * Created with IDEA
 * Creater: MOBIN
 * Date: 2022/4/16
 * Time: 9:33 下午
 */

import com.alibaba.fastjson.JSONObject;
import com.mobin.flink.connectors.redis.RedisDynamicTableFactory;
import com.mobin.flink.connectors.redis.common.config.FlinkJedisClusterConfig;
import com.mobin.flink.connectors.redis.common.config.FlinkJedisConfigBase;
import com.mobin.flink.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.configuration.ReadableConfig;

import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.util.LinkedHashSet;
import java.util.Set;

public class Util {
    public static void checkArgument(boolean condition, String message) {
        if(!condition) {
            throw new IllegalArgumentException(message);
        }
    }

    public static FlinkJedisConfigBase getFlinkJedisConfig(ReadableConfig config) {
        FlinkJedisConfigBase build;
        switch (config.get(RedisDynamicTableFactory.MODE)) {
            case "single":
                build = new FlinkJedisPoolConfig.Builder()
                        .setHost(config.get(RedisDynamicTableFactory.HOST))
                        .setPort(config.get(RedisDynamicTableFactory.PORT))
                        .setPassword(config.get(RedisDynamicTableFactory.PASSWORD))
                        .setMaxIdle(config.get(RedisDynamicTableFactory.CONNECTION_MAX_IDELE))
                        .setMaxTotal(config.get(RedisDynamicTableFactory.CONNECTION_MAX_TOTAL))
                        .setMinIdle(config.get(RedisDynamicTableFactory.CONNECTION_MIN_IDELE))
                        .setTimeout(config.get(RedisDynamicTableFactory.CONNECTION_TIMEOUT))
                        .build();
                break;
            case "cluster":
                build = new FlinkJedisClusterConfig.Builder()
                        .setNodes(getNodes(config))
                        .setPassword(config.get(RedisDynamicTableFactory.PASSWORD))
                        .setMaxIdle(config.get(RedisDynamicTableFactory.CONNECTION_MAX_IDELE))
                        .setMaxTotal(config.get(RedisDynamicTableFactory.CONNECTION_MAX_TOTAL))
                        .setMinIdle(config.get(RedisDynamicTableFactory.CONNECTION_MIN_IDELE))
                        .setMaxRedirections(config.get(RedisDynamicTableFactory.CONNECTION_MAX_REDIRECTIONS))
                        .setTimeout(config.get(RedisDynamicTableFactory.CONNECTION_TIMEOUT))
                        .build();
                break;
            default:
                throw new IllegalArgumentException("Invalid Redis mode. Must be single/cluster");
        }
        return build;
    }

    public static Set<InetSocketAddress> getNodes(ReadableConfig config){
        String ipStr = config.get(RedisDynamicTableFactory.HOST).trim();
        Set<InetSocketAddress> nodes = new LinkedHashSet<>();
        String[] ipList = ipStr.split(",",-1);
        if (ipList.length >= 3) {
            for (String ip: ipList) {
                nodes.add(InetSocketAddress.createUnresolved(ip, config.get(RedisDynamicTableFactory.PORT)));
            }
        } else {
            try {
                throw new MalformedURLException("redis nodes must at least 3");
            } catch (MalformedURLException e) {
                e.printStackTrace();
            }
        }
        return nodes;
    }

    public static String parseJson(String filed, String json) {
        if (JSONObject.parseObject(json) != null) {
            return JSONObject.parseObject(json).getString(filed);
        }
        return null;
    }
}

