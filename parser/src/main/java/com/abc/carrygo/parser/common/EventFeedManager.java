package com.abc.carrygo.parser.common;

import com.abc.carrygo.common.Constants;
import com.abc.carrygo.parser.common.entity.EventFeed;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by plin on 10/19/16.
 */
public class EventFeedManager {
    protected static volatile ConcurrentHashMap<String, ArrayBlockingQueue<EventFeed>> blockingQueueMap;

    static {
        blockingQueueMap = new ConcurrentHashMap<>();
    }

    protected void add(String type, EventFeed eventFeed) throws InterruptedException {
        ArrayBlockingQueue queue = new ArrayBlockingQueue(Constants.CAPACITY);
        queue.put(eventFeed);

        blockingQueueMap.putIfAbsent(type, queue);

        ArrayBlockingQueue actualQueue = blockingQueueMap.get(type);
        if (actualQueue != queue) {
            actualQueue.put(eventFeed);
        }

    }

    protected ArrayBlockingQueue get(String type) {
        ArrayBlockingQueue blockingQueue = blockingQueueMap.get(type);

        return blockingQueue;
    }

    protected boolean isExists(String type) {
        return blockingQueueMap.containsKey(type);
    }
}
