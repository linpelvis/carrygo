package com.abc.carrygo.store.hbase;

import com.abc.carrygo.protocol.SQLEventEntry.*;

/**
 * Created by plin on 10/25/16.
 */
public class HBaseWriterBase {
    protected static DDLEntry.Builder ddlBuilder;
    protected static DMLEntry.Builder dmlBuilder;
    protected static SQLEntry.Builder builder;

    public void init() {
        ddlBuilder = DDLEntry.newBuilder();
        dmlBuilder = DMLEntry.newBuilder();
        builder = SQLEntry.newBuilder();
    }


    public SQLEntry buildSQLEntry(DDLEntry ddlEntry) {
        SQLEntry sqlEntry = builder
                .setEntry(EntryType.DDL)
                .addDdlEntries(ddlEntry)
                .build();
        return sqlEntry;
    }

    public SQLEntry buildSQLEntry(DMLEntry dmlEntry) {
        SQLEntry sqlEntry = builder
                .setEntry(EntryType.DML)
                .addDmlEntries(dmlEntry)
                .build();
        return sqlEntry;
    }

    public DDLEntry buildDDLEntry(KeyWord keyWord, DDLCol col) {
        DDLEntry ddlEntry = ddlBuilder
                .setKeyword(keyWord)
                .addDdlCols(col)
                .build();

        return ddlEntry;
    }

    public DMLEntry buildDMLEntry(KeyWord keyWord, DMLCol col) {
        DMLEntry dmlEntry = dmlBuilder
                .setKeyword(keyWord)
                .addDmlCols(col)
                .build();

        return dmlEntry;
    }

    public DDLCol buildDDLCol(String colName, KeyWord keyWord, boolean isKey) {
        DDLCol ddlCol = DDLCol.newBuilder()
                .setCol(colName)
                .setIsKey(isKey)
                .setKeyword(keyWord)
                .build();

        return ddlCol;
    }

    public DMLCol buildDMLCol(String colName,
                              String value,
                              boolean isUpdate,
                              boolean isKey) {
        DMLCol dmlCol = DMLCol.newBuilder()
                .setCol(colName)
                .setIsKey(isKey)
                .setIsUpdate(isUpdate)
                .setValue(value)
                .build();

        return dmlCol;
    }
}
