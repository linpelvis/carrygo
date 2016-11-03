package com.abc.carrygo.common.utils;

import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;

public class ConcurrentUtil {

    private static Thread.UncaughtExceptionHandler handler = new Thread.UncaughtExceptionHandler() {
        public void uncaughtException(Thread t, Throwable e) {
            System.err.print("thread has an error!!!" + "thread:" + t.getName() + "." + e);
        }
    };

    public static ThreadFactory getNamedThreadFactory(String name) {
        return getNamedThreadFactory(name, false);
    }

    public static ThreadFactory getNamedThreadFactory(String name, boolean isDaemon) {
        ThreadFactoryBuilder builder = new ThreadFactoryBuilder();
        builder
                .setDaemon(isDaemon)
                .setNameFormat(name + "-thread-%d")
                .setUncaughtExceptionHandler(handler);
        return builder.build();
    }

    public static ListeningExecutorService getListeningExecutorService(
            int nThreads, ThreadFactory threadFactory) {
        ExecutorService es = Executors.newFixedThreadPool(nThreads, threadFactory);
        return getListeningExecutorService(es);
    }

    public static ListeningExecutorService getListeningExecutorService(
            ExecutorService executorService) {
        return MoreExecutors.listeningDecorator(executorService);
    }

    public static ScheduledExecutorService getScheduledExecutorService(int corePoolSize) {
        ThreadFactory threadFactory = getNamedThreadFactory("SCHEDULED");
        return getScheduledExecutorService(corePoolSize, threadFactory);
    }

    public static ScheduledExecutorService getScheduledExecutorService(
            int corePoolSize, ThreadFactory threadFactory) {
        return Executors.newScheduledThreadPool(corePoolSize, threadFactory);
    }
}
