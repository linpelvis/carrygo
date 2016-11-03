package com.abc.carrygo.common.kafka;

import com.abc.carrygo.common.zookeeper.ZkAddress;
import kafka.consumer.ConsumerConfig;
import kafka.javaapi.consumer.ConsumerConnector;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

/**
 * Created by plin on 9/26/16.
 */
public class KafkaConnectors {
    private static volatile KafkaConnectors instance = new KafkaConnectors();

    private KafkaConnectors() {
    }

    public static KafkaConnectors getInstance() {
        return instance;
    }

    public static Producer newProducer(KafkaAddress kafkaAddress) {
        return initProducer(kafkaAddress);
    }

    private static Producer<String, byte[]> initProducer(KafkaAddress kafkaAddress) {
        Properties props = new Properties();
        //分组partition, compression
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaAddress.getKafkaServers());
        props.setProperty(ProducerConfig.ACKS_CONFIG, "1");
//        props.setProperty(ProducerConfig.RETRIES_CONFIG, "3");
//        props.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, "16384");
//        props.setProperty(ProducerConfig.LINGER_MS_CONFIG, "1");
//        props.setProperty(ProducerConfig.BUFFER_MEMORY_CONFIG, "33554432");
//        props.setProperty(ProducerConfig.METADATA_FETCH_TIMEOUT_CONFIG, "300");
//        props.setProperty(ProducerConfig.TIMEOUT_CONFIG, "300");
//        props.setProperty(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, "10000");
//        props.setProperty(ProducerConfig.RECONNECT_BACKOFF_MS_CONFIG, "10000");
        props.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");

        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.ByteArraySerializer");

//        props.setProperty("partitioner.class", "com.abc.carrygo.common.kafka.RecordPartitioner");

        return new KafkaProducer<>(props);
    }

    public static ConsumerConnector newConsumer(ZkAddress zkAddress, String groupId) {
        return initConsumer(zkAddress, groupId);
    }

    private static ConsumerConnector initConsumer(ZkAddress zkAddress, String groupId) {
        return kafka.consumer.Consumer.createJavaConsumerConnector(
                createConsumerConfig(zkAddress.getZkServers(), groupId, true));
    }

    private static ConsumerConfig createConsumerConfig(String servers, String groupId, boolean isBeginning) {
        Properties props = new Properties();
        props.put("zookeeper.connect", servers);
        props.put("group.id", groupId);
        props.put("zookeeper.session.timeout.ms", "3000");
        props.put("zookeeper.sync.time.ms", "1000");
        props.put("auto.commit.enable", "true");
        props.put("auto.commit.interval.ms", "1000");
//        props.put("queued.max.message.chunks", "50");
//        props.put("rebalance.max.retries", "5");
        if (isBeginning) {
            props.put("auto.offset.reset", "smallest");
        } else {
            props.put("auto.offset.reset", "latest");
        }

        return new ConsumerConfig(props);
    }


}
