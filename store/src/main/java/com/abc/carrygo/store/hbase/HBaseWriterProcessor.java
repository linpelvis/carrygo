package com.abc.carrygo.store.hbase;

import com.abc.carrygo.common.kafka.KafkaAcceptor;
import com.abc.carrygo.store.AbstractWriterProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Created by plin on 10/17/16.
 */
public class HBaseWriterProcessor extends AbstractWriterProcessor {
    private static final Logger log = LoggerFactory.getLogger(HBaseWriterProcessor.class);
    private static final int N_THREADS = 1;
    private HBaseWriter writer;
    private List<KafkaAcceptor> acceptors;

    public void process() {

        try {
            for (KafkaAcceptor acceptor : acceptors) {
                acceptor.run(N_THREADS, writer, MessageReceived.class.getName());
            }
        } catch (Exception e) {
            log.error("hbase event writer failed.", e);
        }
    }

    public void close() {

        for (KafkaAcceptor acceptor : acceptors) {
            acceptor.close();
        }

        if (writer != null) {
            try {
                writer.close();
            } catch (Exception e1) {
                log.error("hbase event writer close failed.", e1);
            }
        }
    }

    public void setAcceptors(List<KafkaAcceptor> acceptors) {
        this.acceptors = acceptors;
    }

    public void setWriter(HBaseWriter writer) {
        this.writer = writer;
    }
}
