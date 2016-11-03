package com.abc.carrygo.parser.kafka.mysql;

import com.abc.carrygo.protocol.SQLEventEntry.DMLEntry;
import com.abc.carrygo.protocol.SQLEventEntry.KeyWord;
import com.alibaba.otter.canal.protocol.CanalEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static com.abc.carrygo.parser.common.util.ParserUtil.columnFilter;


/**
 * Created by plin on 9/26/16.
 */
public class MySQLDMLEvent {

    private static final Logger log = LoggerFactory.getLogger(MySQLDMLEvent.class);

    public static DMLEntry filter(final String schemaName,
                                  final String tableName,
                                  List<CanalEntry.Column> columns,
                                  final KeyWord keyWord) {
        return columnFilter(schemaName, tableName, keyWord, columns);
    }
}
