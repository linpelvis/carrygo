package com.abc.carrygo.common.kafka;

import com.google.common.base.Joiner;

/**
 * Created by plin on 9/30/16.
 */
public class KafkaAddress {
    private static final Joiner PATH_JOINER = Joiner.on(":").skipNulls();

    private String kafkaIp;
    private String kafkaPort;

    // use spring conf
    public KafkaAddress() {
    }

    public KafkaAddress(String kafkaIp, String kafkaPort) {
        this.kafkaIp = kafkaIp;
        this.kafkaPort = kafkaPort;
    }

    public void setKafkaPort(String kafkaPort) {
        this.kafkaPort = kafkaPort;
    }

    public void setKafkaIp(String kafkaIp) {
        this.kafkaIp = kafkaIp;
    }

    public String getKafkaServers() {
        return PATH_JOINER.join(kafkaIp, kafkaPort);
    }
}
