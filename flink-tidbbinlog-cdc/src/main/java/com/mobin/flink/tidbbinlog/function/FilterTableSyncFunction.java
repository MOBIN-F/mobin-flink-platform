package com.mobin.flink.tidbbinlog.function;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.Sets;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

public class FilterTableSyncFunction extends BroadcastProcessFunction<HashMap<String, Tuple2<HashSet<String>,List<Object>>>, HashMap<String, String>, String> {
    private MapStateDescriptor<String, String> mapStateDescriptor = new MapStateDescriptor<String, String>("tableNames", Types.STRING, Types.STRING);

    @Override
    public void processElement(HashMap<String, Tuple2<HashSet<String>,List<Object>>> value, ReadOnlyContext ctx, Collector<String> out) throws Exception {
        ctx.getBroadcastState(mapStateDescriptor).immutableEntries().forEach(x -> {
            String dbtables = x.getKey();
            Tuple2<HashSet<String>, List<Object>> tableDatas = value.get(dbtables);
            if (tableDatas != null) {
                List<Object> columnValues = tableDatas.f1;
                HashSet<String> columnSchema_record = tableDatas.f0;
                if (x.getValue() == null) { //新增同步表

                } else {
                    HashSet<String> columnSchema_mysql = Sets.newHashSet(Splitter.on(",").splitToList(x.getValue()));
                    Sets.SetView<String> difference = Sets.difference(columnSchema_record, columnSchema_mysql);
                    if (difference != null) { //有新增列

                    }

                }

                if (columnValues != null) {
                    out.collect(Joiner.on(",").useForNull("").join(columnValues));
                }
            }

        });
    }

    @Override
    public void processBroadcastElement(HashMap<String, String> value, Context ctx, Collector<String> out) throws Exception {
        BroadcastState<String, String> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        broadcastState.clear();
        broadcastState.putAll(value);
    }
}
