package com.abc.carrygo.parser.server;

import com.abc.carrygo.common.conf.Configured;
import com.abc.carrygo.common.kafka.KafkaAddress;
import com.abc.carrygo.common.zookeeper.ZkAddress;
import com.abc.carrygo.parser.kafka.service.KafkaEventService;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by plin on 9/30/16.
 */
public class CarryServer {
    private static final Logger log = LoggerFactory.getLogger(CarryServer.class);
    private static ZkAddress zkAddress;
    private static KafkaAddress kafkaAddress;
    private KafkaEventService service;

    @SuppressWarnings("resource")
    public static void main(String[] args) {
//        ZkAddress zkAddress = SpringConf.zkAddress;
//        KafkaAddress kafkaAddress = SpringConf.kafkaAddress;

        zkAddress = Configured.zkAddress;
        kafkaAddress = Configured.kafkaAddress;

        final CarryServer server = new CarryServer();
        server.start();

        Runtime.getRuntime().addShutdownHook(new Thread() {

            public void run() {
                try {

                    server.stop();

                } catch (Throwable e) {
                    log.warn("something goes wrong when stopping carry server:\n{}", ExceptionUtils.getFullStackTrace(e));
                } finally {
                    log.info("carry server is down.");
                }
            }

        });
    }

    public void start() {
        log.info("start the carry server");

        service = new KafkaEventService(
                zkAddress.getZkServers(),
                zkAddress.getZkDestination(),
                kafkaAddress,
                Configured.kafkaTopic);

        service.startParse();
        service.startPublish();
    }

    public void stop() {
        log.info("stop the carry server");

        service.stopParse();
        service.stopPublish();
    }
}
