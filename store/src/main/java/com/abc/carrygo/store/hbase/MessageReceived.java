package com.abc.carrygo.store.hbase;

import com.abc.carrygo.common.kafka.KafkaConsumer;
import com.abc.carrygo.common.serde.ProtobufSerde;
import com.abc.carrygo.protocol.SQLEventEntry.SQLEntry;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by plin on 10/18/16.
 */
public class MessageReceived extends KafkaConsumer {
    private static final Logger log = LoggerFactory.getLogger(MessageReceived.class);

    private HBaseWriter writer;

    public MessageReceived(HBaseWriter writer, KafkaStream stream) {
        super(stream);
        this.writer = writer;
    }

    @Override
    public void run() {

        ConsumerIterator<byte[], byte[]> it = stream.iterator();
        while (it.hasNext()) {
//                log.info("msg:{}", new String(it.next().message()));

            byte[] bytes = it.next().message();
            SQLEntry sqlEntry = ProtobufSerde.fromBinary(bytes, SQLEntry.getDefaultInstance());
            writer.write(sqlEntry);
        }
    }
}
