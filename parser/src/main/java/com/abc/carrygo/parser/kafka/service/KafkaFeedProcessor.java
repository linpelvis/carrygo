package com.abc.carrygo.parser.kafka.service;

import com.abc.carrygo.common.factory.ListeningExecutorFactory;
import com.abc.carrygo.parser.common.entity.Event;
import com.abc.carrygo.parser.kafka.feed.KafkaFeed;
import com.abc.carrygo.parser.kafka.feed.KafkaFeedTask;
import com.abc.carrygo.parser.kafka.manager.KafkaFeedManager;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * Created by plin on 10/19/16.
 */
public class KafkaFeedProcessor {
    private static final Logger log = LoggerFactory.getLogger(KafkaFeedProcessor.class);

    public List topics;
    private ListeningExecutorFactory executor;
    private KafkaEventHandler handler;
    private KafkaFeedManager feedMgr;

    public KafkaFeedProcessor(List topics) {
        this.topics = topics;
    }

    public void extract(long batchId, Map<String, List<Event>> eventMaps) throws InterruptedException {
        for (String topic : eventMaps.keySet()) {

            if (StringUtils.isNotEmpty(topic) && topics.contains(topic)) {
                KafkaFeed eventFeed = new KafkaFeed(batchId,
                        topic,
                        eventMaps.get(topic));

                log.info("extract, batchId:{}", batchId);

                // TODO: 10/24/16 a topic -> a thread
                feedMgr.fill(eventFeed);

            } else {
                return;
            }

        }
    }


    public void start() {

        for (Object topic : topics) {
            log.info("start process task, topic:{}", topic);

            KafkaFeedTask task = new KafkaFeedTask((String) topic);
            task.setFeedManager(feedMgr);
            task.setHandler(handler);

            executor.execute(task);
        }
    }


    public void stop() {


        try {
            executor.shutdown();
        } catch (Exception e) {
            log.error("stop error.", e);

            executor.shutdownNow();
            Thread.currentThread().interrupt();
        } finally {
            handler.release();
        }
    }


    public void setFeedManager(KafkaFeedManager feedMgr) {
        this.feedMgr = feedMgr;
    }

    public void setHandler(KafkaEventHandler handler) {
        this.handler = handler;
    }

    public void setExecutorFactory(ListeningExecutorFactory executor) {
        this.executor = executor;
    }

}
