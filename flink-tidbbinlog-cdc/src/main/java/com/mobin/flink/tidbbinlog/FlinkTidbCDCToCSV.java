package com.mobin.flink.tidbbinlog;

import com.google.common.base.Splitter;
import com.mobin.flink.tidbbinlog.function.FilterTableSyncFunction;
import com.mobin.flink.tidbbinlog.function.MySQLRichFunction;
import com.mobin.flink.tidbbinlog.function.ParseTiDBBinlogFunction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.NullByteKeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.mobin.flink.tidbbinlog.utils.FlinkTidbCDCOptions.*;


public class FlinkTidbCDCToCSV {

    public static class TidbCDCBucketAssigner implements BucketAssigner<String, String>{

        @Override
        public String getBucketId(String record, Context context) {
            List<String> strs = Splitter.on(",").splitToList(record);
            DateTimeFormatter df = DateTimeFormatter.ofPattern("yyyy-MM-dd");
            String format = df.format(LocalDateTime.parse(strs.get(strs.size() - 3)));
            String path = strs.get(strs.size() - 2) + "/" + strs.get(strs.size() - 1) + "/pt=" + format; //dbName/tbName/pt=yyyy-mm-dd
            return path;
        }
        @Override
        public SimpleVersionedSerializer<String> getSerializer() {
            return SimpleVersionedStringSerializer.INSTANCE;
        }
    }

    private static class ByteArrayDeserializationSchema extends AbstractDeserializationSchema<byte[]> {
        @Override
        public byte[] deserialize(byte[] bytes) {
            return bytes;
        }
    }

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .useBlinkPlanner()
                .build();
        StreamTableEnvironment streamTabEnv = StreamTableEnvironment.create(env, settings);

        env.enableCheckpointing(FLINK_CHECKPOINT_INTERVAL_MIN, CheckpointingMode.EXACTLY_ONCE);
        KafkaSource<byte[]> kafkaSource = KafkaSource.<byte[]>builder()
                .setBootstrapServers(KAFKA_BOOTSTRAP_SERVER)
                .setTopics(KAFKA_TOPIC)
                .setGroupId(KAFKA_GROUP_ID)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new ByteArrayDeserializationSchema())
                .build();

        MapStateDescriptor<String, String> mapStateDescriptor = new MapStateDescriptor<String, String>("tableNames", Types.STRING, Types.STRING);

        BroadcastStream<HashMap<String, String>> broadcastStreaming = env.addSource(new MySQLRichFunction()).broadcast(mapStateDescriptor);

        DataStreamSource<byte[]> kafkaSourceStreaming = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafka_source");

        SingleOutputStreamOperator<HashMap<String, Tuple2<HashSet<String>,List<Object>>>> parseTiDBBinlogStreaming = kafkaSourceStreaming.flatMap(new ParseTiDBBinlogFunction()).uid("parseTiDBBinlog");

        SingleOutputStreamOperator<String> tidbDataOperator = parseTiDBBinlogStreaming.keyBy(new NullByteKeySelector<>()).connect(broadcastStreaming).process(new FilterTableSyncFunction()).uid("tidbDataOperator");

        final FileSink<String> hdfsSink = FileSink
                .forRowFormat(new Path(TIDBBINLOG_DFS_BASE_PATH), new SimpleStringEncoder<String>("UTF-8"))
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                .withRolloverInterval(TimeUnit.MINUTES.toMillis(15))
                                .withInactivityInterval(TimeUnit.MINUTES.toMillis(5))
                                .withMaxPartSize(268435456) //256MB
                                .build())
                .withBucketAssigner(new TidbCDCBucketAssigner())
                .build();

        tidbDataOperator.sinkTo(hdfsSink).name("TidbBinlogToHDFS");
        env.execute("FlinkTidbCDCToCSV");
    }
}