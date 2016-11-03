package com.abc.carrygo.store.hbase.service;

import com.abc.carrygo.common.hbase.HBaseConnector;
import com.abc.carrygo.common.kafka.KafkaAcceptor;
import com.abc.carrygo.common.zookeeper.ZkAddress;
import com.abc.carrygo.store.hbase.HBaseWriter;
import com.abc.carrygo.store.hbase.HBaseWriterProcessor;
import com.abc.carrygo.store.hbase.HBaseWriterProcessorBuilder;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by plin on 10/20/16.
 */
public class HBaseWriterService {
    private static final Logger log = LoggerFactory.getLogger(HBaseWriterService.class);

    private static final String SEPARATOR_CHAR = ",";


    private HBaseWriterProcessor processor;

    public void init(ZkAddress zkAddress, String specifiedTopics) {
        try {
            HBaseConnector.init(zkAddress);
        } catch (IOException e) {
            log.error("hbase connector init failed.", e);
        }

        String[] strings = StringUtils.split(specifiedTopics, SEPARATOR_CHAR);
        List<String> topics = Arrays.asList(strings);

        List<KafkaAcceptor> acceptors = new ArrayList<>(topics.size());
        for (String topic : topics) {
            KafkaAcceptor acceptor = new KafkaAcceptor(zkAddress, topic);
            acceptors.add(acceptor);
        }


        HBaseWriter writer = new HBaseWriter();
        HBaseWriterProcessorBuilder builder = new HBaseWriterProcessorBuilder();
        builder
                .setAcceptors(acceptors)
                .setWriter(writer);
        this.processor = builder.build();
    }


    public void write() {
        processor.process();
    }

    public void close() {
        processor.close();
    }


}
