package com.abc.carrygo.common.kafka;

import com.abc.carrygo.common.Constants;
import com.abc.carrygo.common.serde.ProtobufSerde;
import org.apache.commons.lang.StringUtils;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Created by plin on 9/26/16.
 */
public final class KafkaPublisher {
    private static final Logger log = LoggerFactory.getLogger(KafkaPublisher.class);

    private final Producer<String, byte[]> producer;

    public KafkaPublisher(Producer producer) {
        this.producer = producer;
    }

    public void send(List<KafkaMessage> kafkaMessages) throws Exception {
        // TODO: 9/29/16 压缩数据v0.2
        if (kafkaMessages == null || kafkaMessages.isEmpty()) {
            log.error("kafkaMessages is empty.");
        }

        for (KafkaMessage kafkaMessage : kafkaMessages) {
            if (StringUtils.isNotEmpty(kafkaMessage.getTopic()) ||
                    StringUtils.isNotEmpty(kafkaMessage.getKey())) {
                ProducerRecord<String, byte[]> producerRecord = new ProducerRecord<>(kafkaMessage.getTopic(),
                        kafkaMessage.getKey(),
                        ProtobufSerde.toBinary(kafkaMessage.getSqlEntry()));

                send(producerRecord);
            }

        }
    }

    private void send(ProducerRecord<String, byte[]> record) throws Exception {
//        log.info("start send msg, topic:{}, key:{}.", record.topic(), record.key());

        int times = 0;

        while (true) {
            try {
                producer.send(record);
                return;
            } catch (Exception e) {
                times += 1;
                if (times > Constants.THREAD_RETRY_TIMES) {
                    throw e;
                } else {
                    try {
                        Thread.sleep(Constants.THREAD_RETRY_INTERVAL);
                    } catch (InterruptedException e1) {
                        throw e1;
                    }
                }
            }
        }
    }

}
