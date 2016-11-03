package com.abc.carrygo.parser.kafka.feed;

import com.abc.carrygo.parser.common.entity.Event;
import com.abc.carrygo.parser.common.entity.EventFeed;

import java.util.List;

/**
 * Created by plin on 10/19/16.
 */
public class KafkaFeed extends EventFeed {
    public final String topic;

    public KafkaFeed(long batchId,
                     String topic,
                     List<Event> events) {
        super(batchId, events);

        this.topic = topic;
    }
}
