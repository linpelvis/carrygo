package com.abc.carrygo.parser.kafka.parse;

import com.abc.carrygo.parser.common.util.EntryParser;
import com.abc.carrygo.protocol.SQLEventEntry.DDLEntry;
import com.abc.carrygo.protocol.SQLEventEntry.DMLEntry;
import com.abc.carrygo.protocol.SQLEventEntry.KeyWord;
import com.alibaba.otter.canal.protocol.CanalEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Created by plin on 10/11/16.
 */
public class KafkaEntryParser extends EntryParser {
    private static final Logger log = LoggerFactory.getLogger(KafkaEntryParser.class);
    private static final KafkaEventFilter filter = new KafkaEventFilter();

    public static List<DDLEntry> parse(String schemaName,
                                       String tableName,
                                       String sql,
                                       CanalEntry.EventType eventType) throws Exception {
        List<DDLEntry> ddlEntries = null;
        switch (eventType) {
            case CREATE:
                ddlEntries = filter.filter(schemaName, tableName, sql, KeyWord.CREATE);
                break;
            case ALTER:
                ddlEntries = filter.filter(schemaName, tableName, sql, KeyWord.ALTER);
                break;
            default:
                //ignore
                log.info("parse entry sql, sql:{}", sql);
        }

        return ddlEntries;
    }

    public static List<DMLEntry> parse(String schemaName,
                                       String tableName,
                                       CanalEntry.RowData rowData,
                                       CanalEntry.EventType eventType) throws Exception {
        List<DMLEntry> dmlEntries = null;
        switch (eventType) {
            case DELETE:
                dmlEntries = filter.filter(schemaName,
                        tableName,
                        rowData.getBeforeColumnsList(),
                        KeyWord.DELETE);
                break;
            case INSERT:
                dmlEntries = filter.filter(schemaName,
                        tableName,
                        rowData.getAfterColumnsList(),
                        KeyWord.INSERT);
                break;
            case UPDATE:
                dmlEntries = filter.filter(schemaName,
                        tableName,
                        rowData.getAfterColumnsList(),
                        KeyWord.UPDATE);
                break;
            default:
                //ignore
                log.info("parse entry row data, rowData:{}", rowData);
        }

        return dmlEntries;
    }
}
