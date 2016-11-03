package com.abc.carrygo.common.conf;

import com.abc.carrygo.common.kafka.KafkaAddress;
import com.abc.carrygo.common.zookeeper.ZkAddress;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.FileSystemXmlApplicationContext;

/**
 * Created by plin on 10/8/16.
 */
public class SpringConf {
    private static final String path = "server/src/main/resources/carrygo-conf.xml";

    public static ZkAddress zkAddress;
    public static KafkaAddress kafkaAddress;
    public static ApplicationContext context;

    static {
        context = new FileSystemXmlApplicationContext(path);
        zkAddress = (ZkAddress) context.getBean("zkAddress");
        kafkaAddress = (KafkaAddress) context.getBean("kafkaAddress");
    }
}
