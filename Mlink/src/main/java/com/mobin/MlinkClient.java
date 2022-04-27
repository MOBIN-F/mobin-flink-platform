package com.mobin;

import com.mobin.cli.CliOptionParser;
import com.mobin.cli.CliOptions;
import com.mobin.cli.CliStatementSplitter;
import org.apache.commons.io.IOUtils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.SqlParserException;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.internal.TableEnvironmentInternal;
import org.apache.flink.table.delegation.Parser;
import org.apache.flink.table.operations.CatalogSinkModifyOperation;
import org.apache.flink.table.operations.ExplainOperation;
import org.apache.flink.table.operations.ModifyOperation;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.command.SetOperation;
import org.apache.flink.table.operations.ddl.CreateTableOperation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;


/**
 * Hello world!
 */
public class MlinkClient {

    private static final Logger LOG = LoggerFactory.getLogger(MlinkClient.class);

    private CliOptions options;

    public MlinkClient(CliOptions options) {
        this.options = options;
    }

    public MlinkClient() {
    }

    public static void main(String[] args) {
        if (args.length == 0) {
            CliOptionParser.helpFormatter();
        }
        final CliOptions options = CliOptionParser.parseClient(args);
        final MlinkClient client = new MlinkClient(options);
        client.start();
    }

    public void start() {
        if (options.getSqlFile() != null) {
            executeInitialization(readFromURL(options.getSqlFile()));
        }
    }

    private String readFromURL(URL file) {
        try {
            return IOUtils.toString(file, StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new MlinkException(
                    String.format("Fail to read content from the %s.",
                            file.getPath()), e);
        }
    }

    private void setDefaultSettings(TableEnvironment tableEnv, boolean isStreaming){
        try {
            ParameterTool active = ParameterTool.fromPropertiesFile(MlinkClient.class.getClassLoader().getResourceAsStream("app.properties"));
            String activeFile = String.format("app-%s.properties", active.get("profiles.active"));
            ParameterTool tool = ParameterTool.fromPropertiesFile(MlinkClient.class.getClassLoader().getResourceAsStream(activeFile));

            Configuration tableConf = tableEnv.getConfig().getConfiguration();
            tableEnv.getConfig().setLocalTimeZone(ZoneOffset.ofHours(8));
            if (isStreaming) {  //流模式才能设置ck相关参数
                tableConf.setString("state.backend", "rocksdb");
                tableConf.setString("state.checkpoints.dir", tool.get("rocks.db.checkpoint.path"));
                tableConf.setString("state.backend.incremental", "true");
                tableConf.setString("execution.checkpointing.interval", "2min");
                tableConf.setString("execution.checkpointing.min-pause", "10s");
                tableConf.setString("execution.checkpointing.externalized-checkpoint-retention", "RETAIN_ON_CANCELLATION");
            }

            tableConf.setBoolean("table.dynamic-table-options.enabled",true);

            tableConf.setString("restart-strategy", "failure-rate");
            tableConf.setString("restart-strategy.failure-rate.delay", "10s");
            tableConf.setString("restart-strategy.failure-rate.failure-rate-interval", "5min");
            tableConf.setInteger("restart-strategy.failure-rate.max-failures-per-interval", 3);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public TableResult executeInitialization(String content) {
        return executeStatement(content);
    }

    private TableResult executeStatement(String content) {
        Tuple2<List<String>, Boolean> statementsAndMode = CliStatementSplitter.splitContent(content);
        EnvironmentSettings settings = statementsAndMode.f1
                ?
                EnvironmentSettings.newInstance()
                        .inStreamingMode()
                        .useBlinkPlanner()
                        .build()
                :
                EnvironmentSettings.newInstance()
                        .inBatchMode()
                        .useBlinkPlanner()
                        .build();
        if (settings.isStreamingMode()) {
            LOG.info("该任务为streaming任务");
        } else {
            LOG.info("该任务为batch任务");
        }
        TableEnvironment tableEnv = TableEnvironment.create(settings);
        setDefaultSettings(tableEnv, statementsAndMode.f1);
        TableEnvironmentInternal tabEnvInternal = (TableEnvironmentInternal) tableEnv;


        List<ModifyOperation> InsertOperations = new ArrayList<>();
        Parser parser = tabEnvInternal.getParser();
        //语法校验
        LOG.info("执行语句：");
        for (String statement : statementsAndMode.f0) {
            LOG.info(statement);
            try {
                Operation opt = parser.parse(statement).get(0);
                if (opt instanceof CatalogSinkModifyOperation) {
                    //添加INSERT
                    InsertOperations.add((ModifyOperation) opt);
                } else if (opt instanceof SetOperation) {
                    //SET
                    callSet((SetOperation) opt, tabEnvInternal);
                } else if (opt instanceof ExplainOperation) {
                    //EXPLAIN
                    callExplain((ExplainOperation) opt, tabEnvInternal, statement);
                } else if (opt instanceof CreateTableOperation) {
                    //CREATE TABLE
                    callCreateTable((CreateTableOperation) opt, tabEnvInternal);
                } else {
                    executOperation(opt, tabEnvInternal);
                }
            } catch (SqlParserException e) {
                e.printStackTrace();
                System.exit(-1);
            }
        }
        //执行INSERT
        if (!InsertOperations.isEmpty()) {
            return callInsert(InsertOperations, tabEnvInternal);
        }
        return null;
    }

    private TableResult callInsert(List<ModifyOperation> InsertOperations, TableEnvironmentInternal tabEnvInternal) {
        TableResult tableResult = tabEnvInternal.executeInternal(InsertOperations);
        return tableResult;
    }

    private void callCreateTable(CreateTableOperation operation, TableEnvironmentInternal tabEnvInternal) {
        tabEnvInternal.executeInternal(operation);
    }

    private void executOperation(Operation operation, TableEnvironmentInternal tabEnvInternal) {
        tabEnvInternal.executeInternal(operation);
    }

    private void callExplain(ExplainOperation operation, TableEnvironmentInternal tabEnvInternal, String statement) {
        TableResult tableResult = tabEnvInternal.executeInternal(operation);
        final String explaination =
                Objects.requireNonNull(tableResult.collect().next().getField(0)).toString();
        LOG.info("【%s】的执行计划：\n" ,statement);
        LOG.info(explaination);
        System.exit(0);
    }

    private void callSet(SetOperation setOperation, TableEnvironmentInternal tabEnvInternal) {
        if (setOperation.getKey().isPresent() && setOperation.getValue().isPresent()) {
            String key = setOperation.getKey().get().trim();
            String value = setOperation.getValue().get().trim();
            tabEnvInternal.getConfig().getConfiguration().setString(key, value);
        }
    }
}
