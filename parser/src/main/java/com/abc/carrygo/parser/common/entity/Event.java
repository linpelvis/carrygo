package com.abc.carrygo.parser.common.entity;

import com.abc.carrygo.protocol.SQLEventEntry;
import com.alibaba.otter.canal.protocol.CanalEntry;


/**
 * Created by plin on 10/11/16.
 */
public class Event {
    //ddl 执行的sql
    String sql;

    //dml 执行的数据
    CanalEntry.RowData rowData;

    String schemaName;

    String tableName;

    //DML/DDL
    SQLEventEntry.EntryType entryType;

    //create, delete, alter...
    CanalEntry.EventType eventType;

    public String getSchemaName() {
        return schemaName;
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    public SQLEventEntry.EntryType getEntryType() {
        return entryType;
    }

    public void setEntryType(SQLEventEntry.EntryType entryType) {
        this.entryType = entryType;
    }

    public CanalEntry.EventType getEventType() {
        return eventType;
    }

    public void setEventType(CanalEntry.EventType eventType) {
        this.eventType = eventType;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public CanalEntry.RowData getRowData() {
        return rowData;
    }

    public void setRowData(CanalEntry.RowData rowData) {
        this.rowData = rowData;
    }

    public String getSql() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }
}


