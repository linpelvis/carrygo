package com.abc.carrygo.store.hbase;

import com.abc.carrygo.common.hbase.HBaseCell;
import com.abc.carrygo.common.hbase.HBaseClient;
import com.abc.carrygo.common.hbase.HBaseTableAdmin;
import com.abc.carrygo.common.writer.AbstractWriter;
import com.abc.carrygo.protocol.SQLEventEntry.*;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by plin on 10/11/16.
 */
public class HBaseWriter extends AbstractWriter {
    private static final Logger log = LoggerFactory.getLogger(HBaseWriter.class);

    private final EventWriter writer;

    public HBaseWriter() {
        writer = new EventWriter();
    }


    private void writeDDLEvent(List<DDLEntry> entries) {
        for (DDLEntry entry : entries) {
            switch (entry.getKeyword()) {
                case CREATE:
                    writer.create(entry);
                    break;
                case ALTER:
                    writer.alter(entry);
                    break;
                default:
                    //ignore
            }
        }
    }

    private void writeDMLEvent(List<DMLEntry> entries) {
        for (DMLEntry entry : entries) {
            switch (entry.getKeyword()) {
                case INSERT:
                case UPDATE:
                    writer.put(entry);
                    break;
                case DELETE:
                    writer.del(entry);
                    break;
                default:
                    //ignore
            }
        }
    }

    public void write(final SQLEntry sqlEntry) {
        try {
            process(new Writer() {
                @Override
                public void message() {
                    switch (sqlEntry.getEntry()) {
                        case DDL:
                            writeDDLEvent(sqlEntry.getDdlEntriesList());
                            break;
                        case DML:
                            writeDMLEvent(sqlEntry.getDmlEntriesList());
                            break;
                        default:
                            //ignore
                    }
                }
            });
        } catch (Exception e) {
            log.error("hbase write error!!!", e);
        }
    }

    @Override
    public void close() throws Exception {
        writer.close();
    }

    public EventWriter getWriter() {
        return writer;
    }

    public class EventWriter implements WriterTransaction<DDLEntry, DMLEntry> {
        private final HBaseClient hBaseClient;
        private HBaseTableAdmin hBaseTableAdmin;

        public EventWriter() {
            this.hBaseTableAdmin = new HBaseTableAdmin();
            this.hBaseClient = new HBaseClient();
        }

        @Override
        public void create(DDLEntry entry) {
            String ns = entry.getDb();
            String tableName = entry.getTable();

            tableName = getNSTable(ns, tableName);

            List<String> cols = new ArrayList<>();
            List<DDLCol> ddlCols = entry.getDdlColsList();
            String rowKey = null;
            for (DDLCol col : ddlCols) {
                if (col.getIsKey() && StringUtils.isEmpty(rowKey)) {
                    rowKey = col.getCol();
                    continue;
                }
                cols.add(col.getCol());
            }

            try {
                if (!hBaseTableAdmin.hasNamespace(ns)) {
                    hBaseTableAdmin.createNamespace(ns);
                }

                hBaseTableAdmin.createTable(tableName, rowKey);
                hBaseTableAdmin.add(tableName, cols);
            } catch (Exception e) {
                log.error("create table failed.", e);
            }

        }

        @Override
        public void alter(DDLEntry entry) {
            String tableName = getNSTable(entry.getDb(), entry.getTable());

            List<String> colsByDelEvent = new ArrayList<>();
            List<String> colsByAddEvent = new ArrayList<>();
            List<DDLCol> ddlCols = entry.getDdlColsList();
            for (DDLCol col : ddlCols) {
                switch (col.getKeyword()) {
                    case DROP:
                        colsByDelEvent.add(col.getCol());
                        break;
                    case ADD:
                        colsByAddEvent.add(col.getChangeCol());
                        break;
                    case CHANGE:
                        log.warn("change col name is not support, table:{}, col:{}, changeCol:{}.",
                                tableName, col.getCol(), col.getChangeCol());
                        break;
                    default:
                        //ignore
                }
            }

            try {
                if (!colsByAddEvent.isEmpty()) {

                    hBaseTableAdmin.add(tableName, colsByAddEvent);
                }

                if (!colsByDelEvent.isEmpty()) {

                    hBaseTableAdmin.del(tableName, colsByDelEvent);
                }

            } catch (Exception e) {
                log.warn("alter table failed. msg:{}", e.getMessage());
            }

        }

        @Override
        public void del(DMLEntry entry) {
            String tableName = getNSTable(entry.getDb(), entry.getTable());

            List<DMLCol> dmlCols = entry.getDmlColsList();
            DMLCol dmlCol = getRowKeyCol(dmlCols);
            String rowKey = dmlCol.getValue();

            try {
                hBaseClient.delete(tableName, rowKey);
            } catch (Exception e) {
                log.warn("del col failed. msg:{}, rowKey:{} ", e.getMessage(), rowKey);
            }
        }

        public void del(String table) {
            try {
                hBaseTableAdmin.delTable(table);
            } catch (Exception e) {
                log.warn("del table failed. msg:{}, table:{} ", e.getMessage(), table);
            }
        }

        public List getTables(String ns) {
            List tables = new ArrayList();

            try {
                tables = hBaseTableAdmin.getTables(ns);
            } catch (Exception e) {
                log.warn("get tables failed. msg:{}, ns:{} ", e.getMessage(), ns);
            }

            return tables;
        }

        @Override
        public void put(DMLEntry entry) {
            String tableName = getNSTable(entry.getDb(), entry.getTable());

            List<DMLCol> dmlCols = entry.getDmlColsList();
            List<HBaseCell> cells = new ArrayList<>();

            DMLCol dmlCol = getRowKeyCol(dmlCols);
            String rowKey = dmlCol.getValue();

            switch (entry.getKeyword()) {
                case INSERT:
                case UPDATE:
                    setHBaseCells(dmlCols, cells, rowKey);
                    break;
                default:
                    //ignore
            }

            try {
                hBaseClient.put(tableName, cells);
            } catch (Exception e) {
                log.warn("put table failed. msg:{}, cells:{} ", e.getMessage(), cells);
            }
        }


        private String getNSTable(String ns, String tableName) {
            StringBuilder sb = new StringBuilder();
            sb.append(ns);
            sb.append(":");
            sb.append(tableName);

            tableName = sb.toString();
            return tableName;
        }

        private DMLCol getRowKeyCol(List<DMLCol> dmlCols) {
            DMLCol dmlCol = null;

            for (DMLCol col : dmlCols) {
                if (col.getIsKey()) {
                    dmlCol = col;
                    break;
                }
            }

            return dmlCol;
        }

        private void setHBaseCells(List<DMLCol> dmlCols, List<HBaseCell> cells, String rowKey) {
            for (DMLCol col : dmlCols) {
                HBaseCell cell = new HBaseCell();
                if (col.getIsUpdate()) {
                    cell.setCol(col.getCol());
                    cell.setColf(col.getCol());
                    cell.setValue(col.getValue());
                    cell.setRowKey(rowKey);

                    cells.add(cell);
                }
            }
        }

        public void close() throws Exception {
            hBaseTableAdmin.close();
            hBaseClient.close();
        }
    }

}
