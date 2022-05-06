package com.mobin.flink.tidbbinlog.function;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;

import static com.mobin.flink.tidbbinlog.utils.FlinkTidbCDCOptions.*;


public class MySQLRichFunction extends RichSourceFunction<HashMap<String, String>> {
    private Connection connection;
    private PreparedStatement preparedStatement;
    private Boolean flag = true;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        Class.forName("com.mysql.jdbc.Driver");
        connection = DriverManager.getConnection(
                MYSQL_URL,
                MYSQL_USER,
                MYSQL_PASSWORD);
        String sql = "SELECT databaseName,tableName,columnSchema FROM tidbTest";
        preparedStatement = connection.prepareStatement(sql);
    }

    @Override
    public void run(SourceContext<HashMap<String, String>> ctx) throws Exception {
        while (flag) {
            HashMap<String, String> maps = new HashMap<>();
            ResultSet resultSet = preparedStatement.executeQuery();
            while (resultSet.next()) {
                String databaseName = resultSet.getString("databaseName");
                String tableName = resultSet.getString("tableName");
                maps.put(databaseName + tableName, null);
            }
            ctx.collect(maps);
            Thread.sleep(MYSQL_INTERVAL_MIN);
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (connection != null) {
            connection.close();
        }
        if (preparedStatement != null) {
            preparedStatement.close();
        }
    }

    @Override
    public void cancel() {
        flag = false;
    }
}
