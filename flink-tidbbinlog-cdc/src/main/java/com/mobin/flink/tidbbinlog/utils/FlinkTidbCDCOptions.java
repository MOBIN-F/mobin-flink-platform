package com.mobin.flink.tidbbinlog.utils;

import org.apache.flink.api.java.utils.ParameterTool;

import java.io.IOException;

public class FlinkTidbCDCOptions {

    private FlinkTidbCDCOptions(){}

    private static ParameterTool tool;

    static {
        try {
            ParameterTool parameterTool = ParameterTool.fromPropertiesFile(FlinkTidbCDCOptions.class.getClassLoader().getResourceAsStream("app.properties"));
            String activeFile = String.format("app-%s.properties", parameterTool.get("profiles.active"));
            tool = ParameterTool.fromPropertiesFile(FlinkTidbCDCOptions.class.getClassLoader().getResourceAsStream(activeFile));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static final String KAFKA_BOOTSTRAP_SERVER = tool.get("kafka.bootstrap.server");

    public static final String KAFKA_TOPIC = tool.get("kafka.topic");

    public static final String KAFKA_GROUP_ID = tool.get("kafka.group.id");

    public static final String MYSQL_URL = tool.get("mysql.url");

    public static final String MYSQL_USER = tool.get("mysql.user");

    public static final String MYSQL_PASSWORD = tool.get("mysql.password");

    public static final String TIDBBINLOG_DFS_BASE_PATH = tool.get("tidbbinlog.dfs.base.path");

    public static final Integer MYSQL_INTERVAL_MIN = tool.getInt("mysql.interval.min")  * 60 * 1000;

    public static final Integer FLINK_CHECKPOINT_INTERVAL_MIN =  tool.getInt("flink.checkpoint.interval.min") * 60 * 1000;

}
