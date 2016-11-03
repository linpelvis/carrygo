package com.abc.carrygo.parser.kafka.parse;

import com.abc.carrygo.parser.AbstractEventFilter;
import com.abc.carrygo.parser.kafka.mysql.MySQLDDLEvent;
import com.abc.carrygo.parser.kafka.mysql.MySQLDMLEvent;
import com.abc.carrygo.protocol.SQLEventEntry.DDLEntry;
import com.abc.carrygo.protocol.SQLEventEntry.DMLEntry;
import com.abc.carrygo.protocol.SQLEventEntry.KeyWord;
import com.alibaba.otter.canal.protocol.CanalEntry;

import java.util.Arrays;
import java.util.List;

/**
 * Created by plin on 9/26/16.
 */
public class KafkaEventFilter extends AbstractEventFilter {

    public List<DDLEntry> filter(final String schemaName,
                                 final String tableName,
                                 final String sql,
                                 final KeyWord keyWord) throws Exception {
        return process(new Event<DDLEntry>() {
            @Override
            public List<DDLEntry> parser() throws Exception {
                return MySQLDDLEvent.filter(schemaName, tableName, sql, keyWord);
            }
        });
    }

    public List<DMLEntry> filter(final String schemaName,
                                 final String tableName,
                                 final List<CanalEntry.Column> columns,
                                 final KeyWord keyWord) throws Exception {
        return process(new Event<DMLEntry>() {
            @Override
            public List<DMLEntry> parser() throws Exception {
                DMLEntry entry = MySQLDMLEvent.filter(schemaName, tableName, columns, keyWord);
                return Arrays.asList(entry);
            }
        });
    }
}
