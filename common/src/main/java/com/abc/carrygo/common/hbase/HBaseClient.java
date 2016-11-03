package com.abc.carrygo.common.hbase;

import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class HBaseClient {
    private final Logger log = LoggerFactory.getLogger(this.getClass());

    public void put(String tableName, HBaseCell cell) throws Exception {
        log.info("put cell, tableName:{}, cell:{}", tableName, JSONObject.toJSON(cell));

        Table table = HBaseConnector.getTable(tableName);

        Put p = new Put(Bytes.toBytes(cell.getRowKey()));
        p.addColumn(Bytes.toBytes(cell.getColf()),
                Bytes.toBytes(cell.getCol()),
                Bytes.toBytes(cell.getValue()));

        put(Arrays.asList(p), table);
    }

    public void put(String tableName, List<HBaseCell> cells) throws Exception {
        log.info("put cells, tableName:{}, cells:{}", tableName, JSONObject.toJSON(cells));

        Table table = HBaseConnector.getTable(tableName);

        List<Put> puts = new ArrayList<>();
        if (cells != null && cells.size() > 0) {

            for (HBaseCell cell : cells) {

                Put p = new Put(Bytes.toBytes(cell.getRowKey()));
                p.addColumn(Bytes.toBytes(cell.getColf()),
                        Bytes.toBytes(cell.getCol() == null ? Constant.DEFAULT_QUALIFIER : cell.getCol()),
                        Bytes.toBytes(cell.getValue() == null ? "" : cell.getValue()));
                puts.add(p);
            }

            put(puts, table);
        } //if(cells != null && cells.size() > 0)
    }


    public void delete(String tableName, String rowKey) throws Exception {
        log.info("delete rowKey, tableName:{}, rowKey:{}", tableName, rowKey);

        Table table = HBaseConnector.getTable(tableName);

        Delete delete = new Delete(rowKey.getBytes());

        try {
            table.delete(delete);
        } catch (Exception e) {
            log.error("delete rowKey " + delete.toString() + " failed.", e);
            throw e;
        } finally {
            if (table != null) {
                try {
                    table.close();
                } catch (Exception e) {
                    log.error("close table" + table.getName() + " failed.", e);
                    throw e;
                }
            }
        }
    }

    public void delete(String tableName, List<String> rowKeys) throws Exception {
        log.info("delete rowKeys, tableName:{}, rowKeys:{}", tableName, rowKeys);

        Table table = HBaseConnector.getTable(tableName);

        List<Delete> deletes = new ArrayList<>();

        for (String rowKey : rowKeys) {
            Delete delete = new Delete(rowKey.getBytes());
            deletes.add(delete);
        }

        delete(deletes, table);
    }

    private void delete(List<Delete> deletes, Table table) throws IOException {
        try {
            table.delete(deletes);
        } catch (Exception e) {
            log.error("delete rowKeys " + deletes.toString() + " failed.", e);
            throw e;
        } finally {
            if (table != null) {
                try {
                    table.close();
                } catch (Exception e) {
                    log.error("close table" + table.getName() + " failed.", e);
                    throw e;
                }
            }
        }
    }

    private void put(List<Put> puts, Table table) throws IOException {
        try {
            table.put(puts);
        } catch (Exception e) {
            log.error("put table " + table.getName() + " failed.", e);
            throw e;
        } finally {
            if (table != null) {
                try {
                    table.close();
                } catch (Exception e) {
                    log.error("close table " + table.getName() + " failed.", e);
                    throw e;
                }
            }
        }
    }

    public void close() throws Exception {
        HBaseConnector.close();
    }
}
