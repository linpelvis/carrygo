package com.abc.carrygo.store.server;

import com.abc.carrygo.common.conf.Configured;
import com.abc.carrygo.common.zookeeper.ZkAddress;
import com.abc.carrygo.store.hbase.service.HBaseWriterService;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by plin on 10/20/16.
 */
public class WriterServer {
    private static final Logger log = LoggerFactory.getLogger(WriterServer.class);

    private static ZkAddress zkAddress;
    private static String specifiedTopics;

    private HBaseWriterService service;

    public static void main(String[] args) {
        zkAddress = Configured.zkAddress;
        specifiedTopics = Configured.kafkaTopic;

        final WriterServer server = new WriterServer();
        server.start();

        Runtime.getRuntime().addShutdownHook(new Thread() {

            public void run() {
                try {
                    log.info("stop the writer server");
                    server.stop();

                } catch (Throwable e) {
                    log.warn("something goes wrong when stopping writer server:\n{}", ExceptionUtils.getFullStackTrace(e));
                } finally {
                    log.info("writer server is down.");
                }
            }
        });
    }

    public void start() {
        log.info("start the writer server");
        startWriter();
    }

    public void startWriter() {
        service = new HBaseWriterService();
        service.init(zkAddress, specifiedTopics);

        service.write();
    }

    public void stop() {
        service.close();
    }

}
