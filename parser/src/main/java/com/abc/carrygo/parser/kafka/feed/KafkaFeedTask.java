package com.abc.carrygo.parser.kafka.feed;


import com.abc.carrygo.common.Constants;
import com.abc.carrygo.parser.kafka.manager.KafkaFeedManager;
import com.abc.carrygo.parser.kafka.service.KafkaEventHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by plin on 10/19/16.
 */
public class KafkaFeedTask implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(KafkaFeedTask.class);

    private KafkaEventHandler handler;
    private KafkaFeedManager feedManager;
    private String topic;

    public KafkaFeedTask(String topic) {
        this.topic = topic;
    }

    @Override
    public void run() {
        while (true) {
            try {

                KafkaFeed event = feedManager.getEventFeed(topic);

                if (event != null) {
                    handler.extractAndPublish(event);
                } else {
                    Thread.sleep(Constants.THREAD_RETRY_INTERVAL);
                }

            } catch (Exception e) {
                log.error("publish failed.", e);
            }
        }
    }

    public void setFeedManager(KafkaFeedManager feedManager) {
        this.feedManager = feedManager;
    }

    public void setHandler(KafkaEventHandler handler) {
        this.handler = handler;
    }
}
