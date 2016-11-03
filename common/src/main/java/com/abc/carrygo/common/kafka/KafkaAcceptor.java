package com.abc.carrygo.common.kafka;

import com.abc.carrygo.common.factory.ListeningExecutorFactory;
import com.abc.carrygo.common.utils.ReflectionUtil;
import com.abc.carrygo.common.writer.AbstractWriter;
import com.abc.carrygo.common.zookeeper.ZkAddress;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Created by plin on 9/26/16.
 */
public class KafkaAcceptor {
    private static final Logger log = LoggerFactory.getLogger(KafkaAcceptor.class);
    private static final String DEFAULT_EXECUTOR_NAME = "kafka-acceptor";
    private static final int numThreads = 1;
    private ConsumerConnector consumerConnector;
    private ListeningExecutorFactory executor;
    private String topic;

    public KafkaAcceptor(ZkAddress zkAddress, String topic) {
        this(zkAddress, topic, zkAddress.getZkDestination());
    }

    public KafkaAcceptor(ZkAddress zkAddress, String topic, String groupId) {
        this.consumerConnector = KafkaConnectors.newConsumer(zkAddress, groupId);
        this.topic = topic;
    }


    public void run(int nThreads, AbstractWriter writer, String theClass) throws Exception {
        Class aClass = Class.forName(theClass);

        Map<String, Integer> topicCountMap = new HashMap<>();

        // one topic one thread
        topicCountMap.put(topic, numThreads);

        try {
            Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumerConnector.createMessageStreams(topicCountMap);

            for (String topic : consumerMap.keySet()) {
                log.info("start acceptor topic:{}", topic);

                List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);

                executor = new ListeningExecutorFactory(nThreads, DEFAULT_EXECUTOR_NAME);

                for (final KafkaStream stream : streams) {
                    executor.submit((Runnable) ReflectionUtil.newInstance(aClass, writer, stream));
                }
            }

        } catch (Exception e) {
            log.error("acceptor msg failed.", e);
            close();

            throw e;
        }

    }


    public void close() {

        if (executor != null) {
            try {
                Thread.sleep(10000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            executor.shutdown();
        }

        if (consumerConnector != null) {
            consumerConnector.shutdown();
        }

        try {
            if (!executor.awaitTermination(5000, TimeUnit.MILLISECONDS)) {
                log.info("Timed out waiting for consumer threads to shut down, exiting uncleanly");
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            log.info("Interrupted during shutdown, exiting uncleanly");
            executor.shutdownNow();
        }
    }
}
