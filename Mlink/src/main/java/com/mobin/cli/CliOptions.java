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
    private final URL ddlFile;
    private final URL dmlFile;
    private final String connectors;

    public CliOptions(URL sqlFile, URL ddlFile, URL dmlFile, String connectors){
        this.sqlFile = sqlFile;
        this.ddlFile = ddlFile;
        this.dmlFile = dmlFile;
        this.connectors = connectors;
    }

    public URL getSqlFile() {
        return sqlFile;
    }

    public URL getDdlFile() {
        return ddlFile;
    }

    public URL getDmlFile() {
        return dmlFile;
    }

    public String getConnectors() {
        return connectors;
    }
}
