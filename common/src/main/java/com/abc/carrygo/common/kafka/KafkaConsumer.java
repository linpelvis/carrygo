package com.abc.carrygo.common.kafka;


import kafka.consumer.KafkaStream;

/**
 * Created by plin on 10/17/16.
 */
public class KafkaConsumer implements Runnable {
    protected KafkaStream stream;

    public KafkaConsumer(KafkaStream stream) {
        this.stream = stream;
    }

    @Override
    public void run() {

    }
}
