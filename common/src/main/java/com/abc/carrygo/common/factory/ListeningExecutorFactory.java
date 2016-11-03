package com.abc.carrygo.common.factory;

import com.abc.carrygo.common.utils.ConcurrentUtil;
import com.google.common.util.concurrent.ForwardingListeningExecutorService;
import com.google.common.util.concurrent.ListeningExecutorService;
import org.apache.commons.lang3.StringUtils;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

public class ListeningExecutorFactory extends ForwardingListeningExecutorService {

    private static final String DEFAULT_THREAD_FACTORY_NAME = "carrygo";
    private static final int DEFAULT_THREADS;

    static {
        DEFAULT_THREADS = Math.max(1,
                Runtime.getRuntime().availableProcessors() * 2);
    }

    private ListeningExecutorService listeningExecutorService;

    public ListeningExecutorFactory() {
        this(DEFAULT_THREADS);
    }

    public ListeningExecutorFactory(int nThreads, String name) {
        this(nThreads > 0 ? nThreads : DEFAULT_THREADS,
                ConcurrentUtil.getNamedThreadFactory(StringUtils.isNotEmpty(name) ? name : DEFAULT_THREAD_FACTORY_NAME));
    }

    public ListeningExecutorFactory(int nThreads) {
        this(nThreads, DEFAULT_THREAD_FACTORY_NAME);
    }

    public ListeningExecutorFactory(int nThreads, ThreadFactory factory) {
        this(Executors.newFixedThreadPool(nThreads, factory));
    }

    public ListeningExecutorFactory(ExecutorService executorService) {
        this.listeningExecutorService = ConcurrentUtil.getListeningExecutorService(executorService);
    }

    @Override
    protected ListeningExecutorService delegate() {
        return this.listeningExecutorService;
    }
}
