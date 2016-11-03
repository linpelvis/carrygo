package com.abc.carrygo.common.kafka;

import com.abc.carrygo.protocol.SQLEventEntry;

import java.io.Serializable;

/**
 * Created by plin on 9/29/16.
 */
public class KafkaMessage implements Serializable {
    private String topic;
    private String key;
    private SQLEventEntry.SQLEntry sqlEntry;

    public KafkaMessage(String topic, String key, SQLEventEntry.SQLEntry sqlEntry) {
        this.topic = topic;
        this.key = key;
        this.sqlEntry = sqlEntry;
    }

    public SQLEventEntry.SQLEntry getSqlEntry() {
        return sqlEntry;
    }


    public String getTopic() {
        return topic;
    }

    public String getKey() {
        return key;
    }
}
