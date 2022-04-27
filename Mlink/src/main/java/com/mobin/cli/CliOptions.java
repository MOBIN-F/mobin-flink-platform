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

    public CliOptions(URL sqlFile){
        this.sqlFile = sqlFile;
    }

    public URL getSqlFile() {
        return sqlFile;
    }
}
