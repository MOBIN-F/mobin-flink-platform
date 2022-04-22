package com.mobin.flink.tidbbinlog.function;

import com.google.common.base.Joiner;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.List;

public class FilterTableSyncFunction extends BroadcastProcessFunction<HashMap<String, List<Object>>, HashMap<String, String>, String> {
    private MapStateDescriptor<String, String> mapStateDescriptor = new MapStateDescriptor<String, String>("tableNames", Types.STRING, Types.STRING);

    @Override
    public void processElement(HashMap<String, List<Object>> value, ReadOnlyContext ctx, Collector<String> out) throws Exception {
        ctx.getBroadcastState(mapStateDescriptor).immutableEntries().forEach(x -> {
            String dbtables = x.getKey();
            List<Object> columnValues = value.get(dbtables);
            if (columnValues != null) {
                out.collect(Joiner.on(",").useForNull("").join(columnValues));
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
