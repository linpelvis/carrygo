package com.abc.carrygo.parser.kafka.mysql;

import com.abc.carrygo.protocol.SQLEventEntry.DDLEntry;
import com.abc.carrygo.protocol.SQLEventEntry.KeyWord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static com.abc.carrygo.parser.common.util.ParserUtil.statementParser;


/**
 * Created by plin on 9/26/16.
 */
public class MySQLDDLEvent {

    private static final Logger log = LoggerFactory.getLogger(MySQLDDLEvent.class);


    //DDL:创建表结构, 新增add, modify(改变表的类型,暂不考虑), 修改change或者删除drop列.
    public static List<DDLEntry> filter(final String schemaName,
                                        final String tableName,
                                        final String sql,
                                        final KeyWord keyWord) {
        List<DDLEntry> ddlEntries = statementParser(schemaName, tableName, keyWord, sql);

        log.debug("parser ddlOperas:{}", ddlEntries);

        return ddlEntries;
    }
}
