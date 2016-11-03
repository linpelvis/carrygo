package com.abc.carrygo.parser.kafka.service;

import com.abc.carrygo.common.kafka.KafkaMessage;
import com.abc.carrygo.common.kafka.KafkaPublisher;
import com.abc.carrygo.parser.common.entity.Event;
import com.abc.carrygo.parser.kafka.feed.KafkaFeed;
import com.abc.carrygo.parser.kafka.parse.KafkaEntryParser;
import com.abc.carrygo.protocol.SQLEventEntry.DDLEntry;
import com.abc.carrygo.protocol.SQLEventEntry.DMLEntry;
import com.abc.carrygo.protocol.SQLEventEntry.SQLEntry;
import org.apache.kafka.clients.producer.Producer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by plin on 10/11/16.
 */
public class KafkaEventHandler {
    private static final Logger log = LoggerFactory.getLogger(KafkaEventHandler.class);

    private final KafkaPublisher kafkaPublisher;
    private final Producer producer;

    public KafkaEventHandler(Producer producer) {
        this.producer = producer;
        this.kafkaPublisher = new KafkaPublisher(producer);
    }

    public void release() {
        producer.close();
    }


    public void extractAndPublish(KafkaFeed feed) throws Exception {
        log.info("extractAndPublish, topic:{}, batchId:{}", feed.topic, feed.batchId);

        List<KafkaMessage> kafkaMessages = new ArrayList<>();

        try {
            extract(feed.topic, feed.event, kafkaMessages);

            kafkaPublisher.send(kafkaMessages);
        } catch (Exception e) {
            log.error("extractAndPublish failed.", e);

            throw e;
        }

    }

    private void extract(String schema, List<Event> events, List<KafkaMessage> kafkaMessages) throws Exception {

        for (Event event : events) {
            SQLEntry.Builder sqlEntry = SQLEntry.newBuilder();

            switch (event.getEntryType()) {
                case DDL:
                    List<DDLEntry> ddlEntries = KafkaEntryParser.parse(schema,
                            event.getTableName(),
                            event.getSql(),
                            event.getEventType());

                    if (ddlEntries != null) {
                        sqlEntry.addAllDdlEntries(ddlEntries);
                        sqlEntry.setEntry(event.getEntryType());
                    }

                    break;
                case DML:
                    List<DMLEntry> dmlEntries = KafkaEntryParser.parse(schema,
                            event.getTableName(),
                            event.getRowData(),
                            event.getEventType());

                    if (dmlEntries != null) {
                        sqlEntry.addAllDmlEntries(dmlEntries);
                        sqlEntry.setEntry(event.getEntryType());
                    }

                    break;
                default:
                    //ignore
            }

            if (sqlEntry.hasEntry()) {
                String key = schema + event.getEntryType();
                kafkaMessages.add(new KafkaMessage(schema, key, sqlEntry.build()));
            }
        }
    }
}
