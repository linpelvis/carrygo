package com.abc.carrygo.parser.kafka.manager;

import com.abc.carrygo.parser.common.EventFeedManager;
import com.abc.carrygo.parser.kafka.PublishCount;
import com.abc.carrygo.parser.kafka.feed.KafkaFeed;

import java.util.concurrent.ArrayBlockingQueue;

/**
 * Created by plin on 10/19/16.
 */
public class KafkaFeedManager extends EventFeedManager {

    public void fill(KafkaFeed feed) throws InterruptedException {
        try {

            add(feed.topic, feed);

            //Record the batchId
            PublishCount.put(feed.batchId);
        } catch (Exception e) {
            throw e;
        }
    }

    public KafkaFeed getEventFeed(String schema) {
        ArrayBlockingQueue eventMessages = get(schema);
        if (eventMessages == null || eventMessages.isEmpty()) {
            return null;
        }

        KafkaFeed eventFeed = (KafkaFeed) eventMessages.poll();
        if (eventFeed == null) {
            return null;
        }

        PublishCount.set(eventFeed.batchId);

        return eventFeed;
    }
}
