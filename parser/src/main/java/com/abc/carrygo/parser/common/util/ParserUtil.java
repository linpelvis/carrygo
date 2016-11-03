package com.abc.carrygo.parser.common.util;

import com.abc.carrygo.protocol.SQLEventEntry.*;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLName;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.statement.SQLAlterTableDropColumnItem;
import com.alibaba.druid.sql.ast.statement.SQLAlterTableItem;
import com.alibaba.druid.sql.ast.statement.SQLColumnDefinition;
import com.alibaba.druid.sql.ast.statement.SQLTableElement;
import com.alibaba.druid.sql.dialect.mysql.ast.MySqlPrimaryKey;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.*;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.otter.canal.protocol.CanalEntry;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by plin on 9/21/16.
 */
public class ParserUtil {
    private static final Logger log = LoggerFactory.getLogger(ParserUtil.class);
    private static final String REGEX = "[`'\"]";
    private static List<String> KEY_WORD = new ArrayList<>();

    static {
        for (KeyWord keyWord : KeyWord.values()) {
            KEY_WORD.add(keyWord.name());
        }
    }

    public static boolean equalMultipleStrings(String str, List<String> list) {
        for (String s : list) {
            if (StringUtils.equalsIgnoreCase(str, s)) {
                return true;
            }
        }
        return false;
    }

    public static DMLEntry columnFilter(final String schemaName,
                                        final String tableName,
                                        final KeyWord keyWord,
                                        List<CanalEntry.Column> columns) {
        DMLEntry.Builder dmlEntryBuilder = DMLEntry.newBuilder()
                .setDb(schemaName)
                .setTable(tableName)
                .setKeyword(keyWord);

        // check key counts, 不允许有多个key的情况.
        List<DMLCol> dmlCols = new ArrayList<>();
        for (CanalEntry.Column column : columns) {
            DMLCol dmlCol = DMLCol.newBuilder()
                    .setCol(column.getName())
                    .setIsKey(column.getIsKey())
                    .setIsUpdate(column.getUpdated())
                    .setValue(column.getValue())
                    .build();
            dmlCols.add(dmlCol);
        }

        dmlEntryBuilder.addAllDmlCols(dmlCols);
        return dmlEntryBuilder.build();
    }

    public static List<DDLEntry> statementParser(final String schemaName,
                                                 final String tableName,
                                                 final KeyWord keyWord,
                                                 final String sql) {
        MySqlStatementParser parser = new MySqlStatementParser(sql);
        List<SQLStatement> statementList = parser.parseStatementList();

        List<DDLEntry> ddlEntries = new ArrayList<>();

        for (SQLStatement sqlStatement : statementList) {
            DDLEntry.Builder ddlEntryBuilder = DDLEntry.newBuilder()
                    .setDb(schemaName)
                    .setTable(tableName)
                    .setKeyword(keyWord);

            List<DDLCol> ddlCols = new ArrayList<>();

            // 0.1:仅考虑alter和create操作
            if (sqlStatement instanceof MySqlAlterTableStatement) {
                MySqlAlterTableStatement stmt = (MySqlAlterTableStatement) sqlStatement;
                List<SQLAlterTableItem> items = stmt.getItems();
                for (SQLAlterTableItem item : items) {
                    DDLCol.Builder ddlCol = DDLCol.newBuilder();

                    if (item instanceof MySqlAlterTableAddColumn) {
                        MySqlAlterTableAddColumn stmtOfAddCol = (MySqlAlterTableAddColumn) item;
                        // mysql语法:每一条add column stmt 一个col
                        for (SQLColumnDefinition column : stmtOfAddCol.getColumns()) {
                            String colName = replaceAll(column.getName().getSimpleName());
                            ddlCol.setKeyword(KeyWord.ADD);
                            ddlCol.setChangeCol(colName);
                        }
                    }

                    if (item instanceof MySqlAlterTableChangeColumn) {
                        MySqlAlterTableChangeColumn stmtOfChangeCol = (MySqlAlterTableChangeColumn) item;
                        String originColName = replaceAll(stmtOfChangeCol.getColumnName().getSimpleName());
                        String newColName = replaceAll(stmtOfChangeCol.getNewColumnDefinition().getName().getSimpleName());
                        // if originColName == newColName
                        if (StringUtils.equals(originColName, newColName)) {
                            continue;
                        }
                        ddlCol.setKeyword(KeyWord.CHANGE);
                        ddlCol.setChangeCol(newColName);
                        ddlCol.setCol(originColName);
                    }

                    if (item instanceof SQLAlterTableDropColumnItem) {
                        SQLAlterTableDropColumnItem stmtOfDropCol = (SQLAlterTableDropColumnItem) item;
                        // mysql语法:每一条drop column stmt 一个col
                        for (SQLName col : stmtOfDropCol.getColumns()) {
                            String colName = replaceAll(col.getSimpleName());
                            ddlCol.setKeyword(KeyWord.DROP);
                            ddlCol.setCol(colName);
                        }
                    }

                    if (ddlCol.hasKeyword()) {
                        ddlCols.add(ddlCol.build());
                    }
                }
            }

            if (sqlStatement instanceof MySqlCreateTableStatement) {
                MySqlCreateTableStatement stmt = (MySqlCreateTableStatement) sqlStatement;
                for (SQLTableElement element : stmt.getTableElementList()) {
                    DDLCol.Builder ddlCol = DDLCol.newBuilder();

                    if (element instanceof MySqlSQLColumnDefinition) {
                        String colName = ((MySqlSQLColumnDefinition) element).getName().getSimpleName();
                        ddlCol.setKeyword(KeyWord.CREATE);
                        ddlCol.setCol(replaceAll(colName));
                    }

                    if (element instanceof MySqlPrimaryKey) {
                        List<SQLExpr> exprList = ((MySqlPrimaryKey) element).getColumns();
                        for (SQLExpr expr : exprList) {
                            if (expr instanceof SQLIdentifierExpr) {
                                String name = ((SQLIdentifierExpr) expr).getName();
                                ddlCol.setKeyword(KeyWord.CREATE);
                                ddlCol.setCol(replaceAll(name));
                                ddlCol.setIsKey(true);

                                // 移除已有的数据,重新添加带key的数据
                                int size = ddlCols.size();
                                for (int i = 0; i < size; i++) {
                                    if (StringUtils.equalsIgnoreCase(name, ddlCols.get(0).getCol())) {
                                        ddlCols.remove(ddlCols.get(0));
                                    }
                                }
                            }
                        }
                    }

                    if (ddlCol.hasKeyword()) {
                        ddlCols.add(ddlCol.build());
                    }
                }
            }

            if (!ddlCols.isEmpty()) {
                ddlEntryBuilder.addAllDdlCols(ddlCols);
                ddlEntries.add(ddlEntryBuilder.build());
            }
        }

        return ddlEntries;
    }

    public static String replaceAll(String s) {
        Pattern pattern = Pattern.compile(REGEX);
        Matcher matcher = pattern.matcher(s);
        String number = matcher.replaceAll("");
        return number;
    }
}
