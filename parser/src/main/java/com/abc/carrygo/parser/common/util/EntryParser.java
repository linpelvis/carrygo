package com.abc.carrygo.parser.common.util;

import com.abc.carrygo.parser.common.entity.Event;
import com.abc.carrygo.protocol.SQLEventEntry.EntryType;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.commons.lang.SystemUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.CollectionUtils;

import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by plin on 9/20/16.
 */
public class EntryParser {
    protected static final String SEP = SystemUtils.LINE_SEPARATOR;
    protected static final String DATE_FORMAT = "yyyy-MM-dd HH:mm:ss";
    private static final Logger log = LoggerFactory.getLogger(EntryParser.class);

    private static String context_format = null;
    private static String row_format = null;
    private static String transaction_format = null;

    static {
        context_format = SEP + "****************************************************" + SEP;
        context_format += "* Batch Id: [{}] ,count : [{}] , memsize : [{}] , Time : {}" + SEP;
        context_format += "* Start : [{}] " + SEP;
        context_format += "* End : [{}] " + SEP;
        context_format += "****************************************************" + SEP;
        row_format = SEP
                + "----------------> binlog[{}:{}] , name[{},{}] , eventType : {} , executeTime : {} , delay : {}ms"
                + SEP;
        transaction_format = SEP + "================> binlog[{}:{}] , executeTime : {} , delay : {}ms" + SEP;
    }

    public static void printSummary(Message message, long batchId, int size) {
        long memorySize = 0;
        for (CanalEntry.Entry entry : message.getEntries()) {
            memorySize += entry.getHeader().getEventLength();
        }

        String startPosition = null;
        String endPosition = null;
        if (!CollectionUtils.isEmpty(message.getEntries())) {
            startPosition = buildPositionForDump(message.getEntries().get(0));
            endPosition = buildPositionForDump(message.getEntries().get(message.getEntries().size() - 1));
        }

        SimpleDateFormat format = new SimpleDateFormat(DATE_FORMAT);
        log.info(context_format, batchId, size, memorySize, format.format(new Date()), startPosition,
                endPosition);
    }

    private static String buildPositionForDump(CanalEntry.Entry entry) {
        long time = entry.getHeader().getExecuteTime();
        Date date = new Date(time);
        SimpleDateFormat format = new SimpleDateFormat(DATE_FORMAT);
        return entry.getHeader().getLogfileName() + ":" + entry.getHeader().getLogfileOffset() + ":"
                + entry.getHeader().getExecuteTime() + "(" + format.format(date) + ")";
    }

    public static void printTransaction(List<CanalEntry.Entry> entrys) {
        for (CanalEntry.Entry entry : entrys) {
            long executeTime = entry.getHeader().getExecuteTime();
            long delayTime = new Date().getTime() - executeTime;

            if (entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONBEGIN
                    || entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONEND) {
                if (entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONBEGIN) {
                    CanalEntry.TransactionBegin begin = null;
                    try {
                        begin = CanalEntry.TransactionBegin.parseFrom(entry.getStoreValue());
                    } catch (InvalidProtocolBufferException e) {
                        throw new RuntimeException("parse event has an error , data:" + entry.toString(), e);
                    }
                    // 打印事务头信息，执行的线程id，事务耗时
                    log.info(transaction_format,
                            entry.getHeader().getLogfileName(),
                            String.valueOf(entry.getHeader().getLogfileOffset()),
                            String.valueOf(entry.getHeader().getExecuteTime()), String.valueOf(delayTime));
                    log.info(" BEGIN ----> Thread id: {}", begin.getThreadId());
                } else if (entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONEND) {
                    CanalEntry.TransactionEnd end = null;
                    try {
                        end = CanalEntry.TransactionEnd.parseFrom(entry.getStoreValue());
                    } catch (InvalidProtocolBufferException e) {
                        throw new RuntimeException("parse event has an error , data:" + entry.toString(), e);
                    }
                    // 打印事务提交信息，事务id
                    log.info("----------------\n");
                    log.info(" END ----> transaction id: {}", end.getTransactionId());
                    log.info(transaction_format,
                            entry.getHeader().getLogfileName(),
                            String.valueOf(entry.getHeader().getLogfileOffset()),
                            String.valueOf(entry.getHeader().getExecuteTime()), String.valueOf(delayTime));
                }
            }
        }
    }

    public static Map<String, List<Event>> parseRowData(List<CanalEntry.Entry> entries, List schemas) throws InterruptedException {
        Map<String, List<Event>> eventSchemaFeeds = new HashMap<>();

        for (CanalEntry.Entry entry : entries) {

            if (entry.getEntryType() == CanalEntry.EntryType.ROWDATA) {
                CanalEntry.RowChange rowChange = null;
                try {
                    rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
                } catch (Exception e) {
                    throw new RuntimeException("parse event has an error , data:" + entry.toString(), e);
                }

                CanalEntry.EventType eventType = rowChange.getEventType();

//                log.info(row_format,
//                        entry.getHeader().getLogfileName(),
//                        String.valueOf(entry.getHeader().getLogfileOffset()), entry.getHeader().getSchemaName(),
//                        entry.getHeader().getTableName(), eventType,
//                        String.valueOf(entry.getHeader().getExecuteTime()), String.valueOf(""));

                if (eventType == CanalEntry.EventType.QUERY) {
                    continue;
                }

                String schema = entry.getHeader().getSchemaName();
                if (schemas != null && !schemas.isEmpty() && !schemas.contains(schema)) {
                    continue;
                }


                String table = entry.getHeader().getTableName();

                List<Event> events;


                if (eventSchemaFeeds.containsKey(schema)) {
                    events = eventSchemaFeeds.get(schema);
                } else {
                    events = new ArrayList<>();
                }

                if (rowChange.getIsDdl()) {
                    Event event = new Event();
                    event.setEntryType(EntryType.DDL);
                    event.setSql(rowChange.getSql());
                    event.setEventType(eventType);
                    event.setTableName(table);
                    event.setSchemaName(schema);

                    events.add(event);

                    synchronized (DataLock.writeDataLock) {
                        eventSchemaFeeds.put(schema, events);
                    }
                }

                for (CanalEntry.RowData rowData : rowChange.getRowDatasList()) {
                    Event event = new Event();
                    event.setEntryType(EntryType.DML);
                    event.setRowData(rowData);
                    event.setEventType(eventType);
                    event.setTableName(table);
                    event.setSchemaName(schema);

                    events.add(event);

                    synchronized (DataLock.writeDataLock) {
                        eventSchemaFeeds.put(schema, events);
                    }
                }
            }
        }

        return eventSchemaFeeds;
    }
}
