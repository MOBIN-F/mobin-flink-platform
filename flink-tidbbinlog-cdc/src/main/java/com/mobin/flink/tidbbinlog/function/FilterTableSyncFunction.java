package com.mobin.flink.tidbbinlog.function;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.Sets;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

public class FilterTableSyncFunction extends KeyedBroadcastProcessFunction<Object,HashMap<String, Tuple2<HashSet<String>,List<Object>>>, HashMap<String, String>, String> {
    private static final Logger LOG = LoggerFactory.getLogger(FilterTableSyncFunction.class);
    private MapStateDescriptor<String, String> mapStateDescriptor = new MapStateDescriptor<String, String>("tableNames", Types.STRING, Types.STRING);
    private MapState<String, HashSet<String>> tableSchemaMapState;


    @Override
    public void open(Configuration parameters) throws Exception {
        tableSchemaMapState = getRuntimeContext().getMapState(new MapStateDescriptor<String, HashSet<String>>("tableSchemaMapState", Types.STRING, TypeInformation.of(new TypeHint<HashSet<String>>() {
            @Override
            public TypeInformation<HashSet<String>> getTypeInfo() {
                return super.getTypeInfo();
            }
        })));
        super.open(parameters);
    }

    @Override
    public void processElement(HashMap<String, Tuple2<HashSet<String>,List<Object>>> value, ReadOnlyContext ctx, Collector<String> out) throws Exception {
        ctx.getBroadcastState(mapStateDescriptor).immutableEntries().forEach(tableSchema -> {
            String dbAndTableName = tableSchema.getKey();
            Tuple2<HashSet<String>, List<Object>> tableData = value.get(dbAndTableName);
            if (tableData != null) {
                List<Object> columnValues = tableData.f1;
                try {
                    HashSet<String> columnSchemaMapState = tableSchemaMapState.get(dbAndTableName);
                    if (tableSchema.getValue() == null && columnSchemaMapState == null) { //说明是需要新同步的表
                        LOG.info("新增同步表同步：{},列Schema为：{}", dbAndTableName,value.get(dbAndTableName).f0);
                        tableSchemaMapState.put(dbAndTableName, value.get(dbAndTableName).f0);
                    } else {  //已经存在columnSchema
                        HashSet<String> columnSchemaFromStateOrMysql = tableSchemaMapState.get(dbAndTableName);//mysql中的columnSchema\
                        if (columnSchemaFromStateOrMysql == null) {
                            columnSchemaFromStateOrMysql = Sets.newHashSet(Splitter.on(",").splitToList(tableSchema.getValue()));//mysql中的columnSchema
                        }
                        HashSet<String> columnSchemaFromRecord = value.get(dbAndTableName).f0;

                        Sets.SetView<String> additionalColumn = Sets.difference(columnSchemaFromRecord, columnSchemaFromStateOrMysql);  //新旧columnSchema比较
                        if (additionalColumn.size() != 0) { //有列增加
                            LOG.info("{}表有新增列：{},新的列schema：{}",dbAndTableName,additionalColumn, columnSchemaFromRecord);
                            tableSchemaMapState.put(dbAndTableName,columnSchemaFromRecord);
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
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
