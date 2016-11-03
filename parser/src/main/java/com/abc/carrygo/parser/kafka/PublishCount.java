package com.abc.carrygo.parser.kafka;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by plin on 10/12/16.
 */
public class PublishCount {
    private static volatile AtomicLong id = new AtomicLong(0);

    public synchronized static void put(long batchId) {
        if (id.get() == 0) {
            id.set(batchId);
        }
    }

    public static void set(long batchId) {
        id.set(batchId);
    }

    public static long get() {
        return id.get();
    }

    public static boolean check() {
        if (PublishCount.get() != 0) {
            return Boolean.TRUE;
        }

        return Boolean.FALSE;
    }
}
