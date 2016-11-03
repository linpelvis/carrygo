package com.abc.carrygo.store.hbase;

import com.abc.carrygo.common.kafka.KafkaAcceptor;
import org.springframework.util.Assert;

import java.util.List;

/**
 * Created by plin on 10/19/16.
 */
public final class HBaseWriterProcessorBuilder {

    private HBaseWriterProcessor processor = null;
    private List<KafkaAcceptor> acceptors = null;
    private HBaseWriter writer = null;

    public HBaseWriterProcessorBuilder() {
    }

    private static HBaseWriterProcessor build(HBaseWriterProcessorBuilder builder) {
        final List<KafkaAcceptor> acceptors = builder.acceptors;
        final HBaseWriter writer = builder.writer;

        final HBaseWriterProcessor processor =
                (builder.processor != null)
                        ? builder.processor
                        : new HBaseWriterProcessor();

        processor.setAcceptors(acceptors);
        processor.setWriter(writer);

        return processor;
    }

    public HBaseWriterProcessorBuilder setAcceptors(List<KafkaAcceptor> acceptors) {
        Assert.notNull(acceptors);
        this.acceptors = acceptors;
        return this;
    }

    public HBaseWriterProcessorBuilder setWriter(HBaseWriter writer) {
        Assert.notNull(writer);
        this.writer = writer;
        return this;
    }

    public HBaseWriterProcessor build() {
        return build(this);
    }
}
