package com.abc.carrygo.parser.kafka.service;

import com.abc.carrygo.common.factory.ListeningExecutorFactory;
import com.abc.carrygo.parser.kafka.manager.KafkaFeedManager;
import org.springframework.util.Assert;

import java.util.List;

/**
 * Created by plin on 10/19/16.
 */
public final class KafkaFeedProcessorBuilder {

    private KafkaFeedManager feedManager = null;
    private ListeningExecutorFactory listeningExecutorFactory = null;
    private KafkaEventHandler eventHandler = null;
    private KafkaFeedProcessor processor = null;

    public KafkaFeedProcessorBuilder() {
    }

    private static KafkaFeedProcessor build(KafkaFeedProcessorBuilder builder, List topics) {
        final KafkaEventHandler eventHandler = builder.eventHandler;
        final ListeningExecutorFactory listeningExecutorFactory = builder.listeningExecutorFactory;
        final KafkaFeedManager feedManager = builder.feedManager;

        final KafkaFeedProcessor processor =
                (builder.processor != null)
                        ? builder.processor
                        : new KafkaFeedProcessor(topics);

        processor.setHandler(eventHandler);
        processor.setFeedManager(feedManager);
        processor.setExecutorFactory(listeningExecutorFactory);

        return processor;
    }

    public KafkaFeedProcessorBuilder setFeedManager(KafkaFeedManager feedManager) {
        Assert.notNull(feedManager);

        this.feedManager = feedManager;

        return this;
    }

    public KafkaFeedProcessorBuilder setListeningExecutorFactory(ListeningExecutorFactory listeningExecutorFactory) {
        Assert.notNull(listeningExecutorFactory);

        this.listeningExecutorFactory = listeningExecutorFactory;

        return this;
    }

    public KafkaFeedProcessorBuilder setEventFactory(KafkaFeedProcessor processor) {
        Assert.notNull(processor);

        this.processor = processor;

        return this;
    }

    public KafkaFeedProcessorBuilder setEventHandler(KafkaEventHandler eventHandler) {
        Assert.notNull(eventHandler);

        this.eventHandler = eventHandler;

        return this;
    }

    public KafkaFeedProcessor build(List topics) {
        return build(this, topics);
    }
}
