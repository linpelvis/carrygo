package com.abc.carrygo.common.conf;

import com.abc.carrygo.common.kafka.KafkaAddress;
import com.abc.carrygo.common.utils.ConfigLoader;
import com.abc.carrygo.common.zookeeper.ZkAddress;

/**
 * Created by plin on 10/8/16.
 */
public class Configured {
    public static ZkAddress zkAddress;
    public static KafkaAddress kafkaAddress;

    public static String kafkaTopic;

    static {
        ConfigLoader configLoader = new ConfigLoader("carrygo.properties");

        zkAddress = new ZkAddress(
                configLoader.getValue("zookeeper.destination"),
                configLoader.getValue("zookeeper.ip"),
                configLoader.getValue("zookeeper.port"));

        kafkaAddress = new KafkaAddress(
                configLoader.getValue("kafka.ip"),
                configLoader.getValue("kafka.port"));

        kafkaTopic = configLoader.getValue("kafka.topic");
    }
}
