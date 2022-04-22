package com.mobin.flink.tidbbinlog.function;

import com.mobin.flink.tidbbinlog.bean.BinLogInfo;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class ParseTiDBBinlogFunction implements FlatMapFunction<byte[], HashMap<String, List<Object>>> {

    private static final Logger logger = LoggerFactory.getLogger(ParseTiDBBinlogFunction.class);

    @Override
    public void flatMap(byte[] value, Collector<HashMap<String, List<Object>>> out) {
        try {
            BinLogInfo.Binlog binlog = BinLogInfo.Binlog.parseFrom(value);
            if (!binlog.getType().equals(BinLogInfo.BinlogType.DML)) {
                return;
            }
            List<BinLogInfo.Table> tablesList = binlog.getDmlData().getTablesList();

            for (BinLogInfo.Table table : tablesList) {
                List<BinLogInfo.TableMutation> mutationsList = table.getMutationsList();
                for (BinLogInfo.TableMutation tableMutation : mutationsList) {

                    HashMap<String, List<Object>> map = new HashMap<>();
                    List<Object> columnValues = new ArrayList<>();
                    List<BinLogInfo.Column> columnsList = tableMutation.getRow().getColumnsList();
                    //操作符
                    columnValues.add(BinLogInfo.MutationType.forNumber(tableMutation.getType().getNumber()).toString().toUpperCase());

                    for (int i = 0; i < columnsList.size(); i ++) {
                        Object columnValue = getColumnValue(columnsList.get(i));
                        columnValues.add(columnValue);
                    }

                    columnValues.add(LocalDateTime.now());
                    columnValues.add(table.getSchemaName());
                    columnValues.add(table.getTableName());

                    map.put(table.getSchemaName() + table.getTableName(), columnValues);
                    out.collect(map);
                }
            }
        } catch (Exception e) {
            logger.error("TiDBBinlog数据解析异常：{}", e);
        }
    }

    private Object getColumnValue(BinLogInfo.Column column) {
        Object columnValue = null;
        if (column.hasBytesValue()) {
            columnValue = column.getBytesValue();
        }
        if (column.hasDoubleValue()) {
            columnValue = column.getDoubleValue();
        }
        if (column.hasInt64Value()) {
            columnValue = column.getInt64Value();
        }
        if (column.hasStringValue()) {
            columnValue = column.getStringValue();
        }
        if (column.hasUint64Value()) {
            columnValue = column.getUint64Value();
        }
        if (column.hasIsNull()) {
            columnValue = null;
        }
        return columnValue;
    }
}
