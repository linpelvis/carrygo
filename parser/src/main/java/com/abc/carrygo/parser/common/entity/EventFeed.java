package com.abc.carrygo.parser.common.entity;

import java.util.List;

/**
 * Created by plin on 10/12/16.
 */
public class EventFeed {

    public final long batchId;
    public final List<Event> event;

    public EventFeed(long batchId, List<Event> event) {
        this.batchId = batchId;
        this.event = event;
    }
}
