package com.mobin.cli;

import java.net.URL;

/**
 * Created with IDEA
 * Creater: MOBIN
 * Date: 2021/9/10
 * Time: 2:55 PM
 */
public class CliOptions {
    private final URL sqlFile;
    private final String connectors;

    public CliOptions(URL sqlFile, String connectors){
        this.sqlFile = sqlFile;
        this.connectors = connectors;
    }

    public URL getSqlFile() {
        return sqlFile;
    }

    public String getConnectors() {
        return connectors;
    }
}
