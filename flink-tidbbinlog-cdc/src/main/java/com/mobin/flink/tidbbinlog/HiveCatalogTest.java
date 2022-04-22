package com.mobin.flink.tidbbinlog;

import org.apache.commons.math3.random.RandomDataGenerator;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.source.datagen.DataGeneratorSource;
import org.apache.flink.streaming.api.functions.source.datagen.RandomGenerator;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.util.Collector;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;

import static com.mobin.flink.tidbbinlog.utils.FlinkTidbCDCOptions.*;

/**
 * Created with IDEA
 * Creater: MOBIN
 * Date: 2022/4/22
 * Time: 2:40 下午
 */
public class HiveCatalogTest {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .useBlinkPlanner()
                .build();
        StreamTableEnvironment streamTabEnv = StreamTableEnvironment.create(env, settings);

        env.setParallelism(1);
        SingleOutputStreamOperator<Tuple2<Integer, Integer>> dataGeneratorSource = env.addSource(new DataGeneratorSource<Tuple2<Integer, Integer>>(new RandomGenerator<Tuple2<Integer, Integer>>() {
            @Override
            public Tuple2<Integer, Integer> next() {
                RandomDataGenerator randomDataGenerator = new RandomDataGenerator();
                Tuple2<Integer, Integer> integerIntegerTuple2 = new Tuple2<>(randomDataGenerator.nextInt(1, 5), randomDataGenerator.nextInt(6, 10));
                try {
                    Thread.sleep(5 * 1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                return integerIntegerTuple2;
            }
        })).returns(new TypeHint<Tuple2<Integer, Integer>>() {
            @Override
            public TypeInformation<Tuple2<Integer, Integer>> getTypeInfo() {
                return super.getTypeInfo();
            }
        });

        MapStateDescriptor<String, String> mapStateDescriptor = new MapStateDescriptor<String, String>("tableNames", Types.STRING, Types.STRING);


        SingleOutputStreamOperator<HashMap<String, String>> mapSingleOutputStreamOperator = dataGeneratorSource.flatMap(new FlatMapFunction<Tuple2<Integer, Integer>, HashMap<String, String>>() {
            @Override
            public void flatMap(Tuple2<Integer, Integer> tuple2, Collector<HashMap<String, String>> collector) throws Exception {
                HashMap<String, String> map = new HashMap<>();
                int dbName = tuple2.f0;
                int tbName = tuple2.f1;
                map.put(dbName + "-" + tbName, "v");
                collector.collect(map);
            }
        });

        BroadcastStream<HashMap<String, String>> broadcastStreaming = env.addSource(new RichSourceFunction<HashMap<String, String>>() {
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
                String sql = "SELECT databaseName,tableName FROM tidbTest";
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
                    System.out.println(maps + "==========");
                    ctx.collect(maps);
                    Thread.sleep(1 * 60* 1000);
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
        }).broadcast(mapStateDescriptor);


        mapSingleOutputStreamOperator.connect(broadcastStreaming).process(new BroadcastProcessFunction<HashMap<String, String>, HashMap<String, String>, String>() {
            @Override
            public void processElement(HashMap<String, String> value, ReadOnlyContext ctx, Collector<String> out) throws Exception {
                ctx.getBroadcastState(mapStateDescriptor).immutableEntries().forEach(x -> {
                    System.out.println("x: " + x.getKey());
                });
            }

            @Override
            public void processBroadcastElement(HashMap<String, String> value, Context ctx, Collector<String> out) throws Exception {
                BroadcastState<String, String> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
                broadcastState.clear();
                broadcastState.putAll(value);
            }
        }).print();


        final FileSink<String> hdfsSink = FileSink
                .forRowFormat(new Path("file:///tmp/tidb_tmp"), new SimpleStringEncoder<String>("UTF-8"))
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                .withRolloverInterval(TimeUnit.MINUTES.toMillis(15))
                                .withInactivityInterval(TimeUnit.MINUTES.toMillis(5))
                                .withMaxPartSize(268435456) //256MB
                                .build())
                .withBucketAssigner(new BucketAssigner<String, String>() {
                    @Override
                    public String getBucketId(String s, Context context) {
                        return null;
                    }

                    @Override
                    public SimpleVersionedSerializer<String> getSerializer() {
                        return null;
                    }
                })
                .build();

        mapSingleOutputStreamOperator.addSink(new RichSinkFunction<HashMap<String, String>>() {
            @Override
            public void setRuntimeContext(RuntimeContext t) {
                super.setRuntimeContext(t);
            }
        });
        //mapSingleOutputStreamOperator.sinkTo(hdfsSink);
        env.execute().getJobExecutionResult();

        DataStreamSink<Tuple2<Integer, Integer>> print = dataGeneratorSource.print();
        // env.execute("DataGenSource");
        env.executeAsync("dfd");
//        Table table = streamTabEnv.fromDataStream(print);
//        table.execute();

        Catalog catalog = new HiveCatalog("hive","default", "/opt/hive-2.3.3/conf","2.3.3");
        streamTabEnv.registerCatalog("hive",catalog);
        streamTabEnv.useCatalog("hive");
        streamTabEnv.getConfig().setSqlDialect(SqlDialect.HIVE);

        String alterSql = "alter table testcatalog4 add columns(name string)";
        streamTabEnv.executeSql(alterSql);

        String sql = "desc testcatalog4";
        streamTabEnv.executeSql(sql).print();
//        List<String> strings = catalog.listTables("default");
//        System.out.println(strings);
    }
}
