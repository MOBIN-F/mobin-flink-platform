package com.mobin;

import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.*;
import com.mobin.cli.CliStatementSplitter;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.internal.TableEnvironmentInternal;
import org.apache.flink.table.delegation.Parser;
import org.apache.flink.table.operations.ModifyOperation;
import org.apache.flink.table.operations.Operation;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.regex.Pattern;

public class MlinkClientTest {

    @Test
    public void testInsert() throws ExecutionException, InterruptedException {
        String ddl = "CREATE TABLE blackhole_table (\n" +
                "    f0 INT\n" +
                ") WITH (\n" +
                "      'connector' = 'print'\n" +
                "      )";

        String dml = "INSERT INTO blackhole_table select 11";

        String ddl1 = "CREATE TABLE blackhole_table1 (\n" +
                "    f0 INT\n" +
                ") WITH (\n" +
                "      'connector' = 'print'\n" +
                "      )";

        String dml1 = "INSERT INTO blackhole_table1 select 11";

        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .useBlinkPlanner()
                .build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);
        Parser parser = ((TableEnvironmentInternal)tableEnv).getParser();
        Operation ddlOperation = parser.parse(ddl).get(0);
        Operation dmlOperation = parser.parse(dml).get(0);
        Operation ddlOperation1 = parser.parse(ddl1).get(0);
        Operation dmlOperation1 = parser.parse(dml1).get(0);

        ((TableEnvironmentInternal) tableEnv).executeInternal(ddlOperation);
        ((TableEnvironmentInternal) tableEnv).executeInternal(ddlOperation1);

        List<ModifyOperation> InsertOperations = new LinkedList<>();
        InsertOperations.add((ModifyOperation)dmlOperation);
        InsertOperations.add((ModifyOperation)dmlOperation1);

        TableResult tableResult = ((TableEnvironmentInternal) tableEnv).executeInternal(InsertOperations);
        tableResult.getJobClient().get().getJobExecutionResult().get();

    }


    @Test
    public void testCellMonitor() throws ExecutionException, InterruptedException {


        String ddl = "CREATE TABLE cbsCellDetailChangeQueue(\n" +
                "    status TINYINT,\n" +
                "    deleteFlag TINYINT,\n" +
                "    cabinetCode STRING,\n" +
                "    reserveStatus TINYINT,\n" +
                "    bookStatus TINYINT,\n" +
                "    lockStatus TINYINT,\n" +
                "    cellStatus TINYINT,\n" +
                "    businessStatus TINYINT,\n" +
                "    hasException BOOLEAN,\n" +
                "    businessTimeL BIGINT,\n" +
                "    createTimeL BIGINT,\n" +
                "    cellCode STRING,\n" +
                "    procTime AS PROCTIME()\n" +
                ") WITH (\n" +
                "      'connector' = 'kafka',\n" +
                "--       'topic' = 'cellTest',\n" +
                "      'topic' = 'cbsCellDetailChangeQueue',\n" +
                "      'scan.startup.mode' = 'earliest-offset',\n" +
                "      'properties.group.id' = 'DW-TEST-FlinkCellMonitor16',\n" +
                "      'properties.bootstrap.servers' = '10.204.58.157:9092,10.204.58.158:9092,10.204.58.159:9092',\n" +
                "      'format'='json'\n" +
                "      )";

        String ddl1 = "CREATE TABLE testCol (\n" +
                "         cabinetCode  STRING\n" +
                ") WITH (\n" +
                "      'connector' = 'print'\n" +
                "      )";

        String dml1 = "INSERT INTO testCol select cabinetCode FROM cbsCellDetailChangeQueue";

        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .useBlinkPlanner()
                .build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);
        Parser parser = ((TableEnvironmentInternal)tableEnv).getParser();
        Operation ddlOperation = parser.parse(ddl).get(0);
        //  Operation dmlOperation = parser.parse(dml).get(0);
        Operation ddlOperation1 = parser.parse(ddl1).get(0);
        Operation dmlOperation1 = parser.parse(dml1).get(0);

        ((TableEnvironmentInternal) tableEnv).executeInternal(ddlOperation);
        ((TableEnvironmentInternal) tableEnv).executeInternal(ddlOperation1);

        List<ModifyOperation> InsertOperations = new LinkedList<>();
        //  InsertOperations.add((ModifyOperation)dmlOperation);
        InsertOperations.add((ModifyOperation)dmlOperation1);

        TableResult tableResult = ((TableEnvironmentInternal) tableEnv).executeInternal(InsertOperations);
        tableResult.getJobClient().get().getJobExecutionResult().get();

    }

    @Test
    public void t() {
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .useBlinkPlanner()
                .build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);
        TableEnvironmentInternal tableEnvironmentInternal = (TableEnvironmentInternal)tableEnv;
        MlinkClient mlinkClient = new MlinkClient();
        String sql = "CREATE TABLE blackhole_table (\n" +
                "    f0 INT\n" +
                ") WITH (\n" +
                "      'connector' = 'print'\n" +
                "      );\n" +
                "\n" +
                "INSERT INTO blackhole_table select 11;";
        mlinkClient.executeInitialization(sql);
    }

    @Test
    public void replacAll() {
        String MASK = "--.*$";
        String BEGINNING_MASK = "^(\\s)*--.*$";

        String sql = " --abc;";
        boolean b = sql.replaceAll(MASK, "").trim().endsWith(";");
        String s1 = sql.replaceAll(BEGINNING_MASK, "");
        Assert.assertEquals("", s1);
        Assert.assertFalse(b);
    }

    @Test
    public void testMultiset(){
        Multiset<String> multiset = HashMultiset.create();
        multiset.add("c");
        multiset.add("c");
        Assert.assertEquals(multiset.count("c"),2);
    }

    @Test
    public void testSplitter() {
        String a = "a,b,c";
        Splitter splitter = Splitter.on(",");
        Iterable<String> split = splitter.split(a);
        Boolean b = Iterables.contains(split, "a");
        Assert.assertTrue(b);
    }

    @Test
    public void testPadEnd() {
        String s = Strings.padEnd("aa", 4, 'f');
        Assert.assertEquals("aaff", s);
    }

    @Test
    public void testSET() {
        String SET_MSCK = "(SET|set)(\\s+[']?(execution.runtime-mode)[']?\\s*=?\\s*[']?(BATCH|batch)[']?\\s*)?";
        Assert.assertTrue(Pattern.matches(SET_MSCK, "set 'execution.runtime-mode'='batch'"));
        Assert.assertTrue(Pattern.matches(SET_MSCK, "set 'execution.runtime-mode' = 'batch' "));
        Assert.assertTrue(Pattern.matches(SET_MSCK, "set 'execution.runtime-mode'= 'batch' "));
        Assert.assertTrue(Pattern.matches(SET_MSCK, "set execution.runtime-mode = batch "));
        Assert.assertTrue(Pattern.matches(SET_MSCK, "set execution.runtime-mode=batch "));
        Assert.assertTrue(Pattern.matches(SET_MSCK, "SET execution.runtime-mode=batch"));
    }

    @Test
    public void testFunctionsForMap() {
        List<String> lists = Lists.newArrayList("AAAAAAA","BBBB","CCCCCCCCCC");
        Function<String, String> f1 = s -> s.length() <= 5 ? s : s.substring(0,5);

        Function<String, String> f2 = s -> s.toLowerCase();

        Function<String, String> compose = Functions.compose(f1, f2);

        Collection<String> transform = Collections2.transform(lists, compose);
        for (String s: transform) {
            System.out.println(s);
        }
    }

    @Test
    public void testSep() {
        String property = System.getProperty("line.separator");
        System.out.println(String.valueOf(Character.LINE_SEPARATOR).toString());
        System.out.println(property);
    }

    @Test
    public void testStrSplitter() {
        String sql = "insert into test_hudi_result30\n" +
                "SELECT\n" +
                "  dt,\n" +
                "  count(1) cnt,\n" +
                "  sum(orderAmount),--fff\n" +
                "  current_timestamp ts\n" +
                "FROM test_hudi where dt >= '20211222'\n" +
                "group by dt;  ---abc";
        Tuple2<List<String>, Boolean> listBooleanTuple2 = CliStatementSplitter.splitContent(sql);
        System.out.println(listBooleanTuple2.f0);
    }

    @Test
    public void testStrSplit() {
        String str = "aaa; ---fd;;";
        System.out.println(str.substring(0,str.indexOf(";")));
    }
}
