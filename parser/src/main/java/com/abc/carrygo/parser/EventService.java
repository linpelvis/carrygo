package com.abc.carrygo.parser;

import com.abc.carrygo.parser.common.util.EntryParser;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.protocol.Message;
import org.apache.log4j.MDC;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;

/**
 * Created by plin on 9/19/16.
 */
public class EventService {

    protected static final int BATCH_SIZE = 5 * 1024;
    private static final Logger log = LoggerFactory.getLogger(EventService.class);
    protected static Thread.UncaughtExceptionHandler handler = new Thread.UncaughtExceptionHandler() {
        public void uncaughtException(Thread t, Throwable e) {
            System.err.print("thread has an error!!!" + "thread:" + t.getName() + "." + e);
        }
    };
    protected volatile boolean running = false;
    protected CanalConnector connector;
    protected String destination;
    protected Thread thread = null;

    public EventService(String destination) {
        this(destination, null);
    }

    public EventService(String destination, CanalConnector connector) {
        this.destination = destination;
        this.connector = connector;
    }

    public void setConnector(CanalConnector connector) {
        this.connector = connector;
    }

    public void startParse() {
        Assert.notNull(connector, "connector is null");

        thread = new Thread(new Runnable() {
            @Override
            public void run() {
                process();
            }
        });

        thread.setUncaughtExceptionHandler(handler);
        thread.start();

        running = true;
    }

    public void stopParse() {
        if (!running) {
            return;
        }
        running = false;

        if (thread != null) {
            try {
                thread.join();
            } catch (InterruptedException e) {
                //
                log.warn("interrupt event client!");
            }
        }

        MDC.remove("destination");
    }

    public void process() {
        while (running) {
            try {
                MDC.put("destination", destination);
                //protobuf与canal中protobuf版本不一致会导致无法connect问题
                //启动服务时, 连接不稳定,待查canal是否存在问题

                log.info("start connect.");
                connector.connect();

                log.info("start subscribe.");
                connector.subscribe();

                log.info("start parse event.");

                while (running) {
                    Message message = connector.getWithoutAck(BATCH_SIZE);
                    long batchId = message.getId();

                    parseAndPublish(message, batchId);
                }
            } catch (Exception e) {
                log.error("process error!", e);
            } finally {
                connector.disconnect();
                MDC.remove("destination");
            }
        }
    }

    protected void parseAndPublish(Message message, long batchId) {
        try {
            int size = message.getEntries().size();
            if (batchId == -1 || size == 0) {
//                            log.debug("message is null");
            } else {
//                            EntryParser.printSummary(message, batchId, size);
//                            EntryParser.printTransaction(message.getEntries());

                EntryParser.parseRowData(message.getEntries(), null);

                // publish
            }
            connector.ack(batchId);
        } catch (Exception e) {
            log.error("rollback msg, batchId:{}", batchId, e);
            connector.rollback(batchId);
        }
    }
}
