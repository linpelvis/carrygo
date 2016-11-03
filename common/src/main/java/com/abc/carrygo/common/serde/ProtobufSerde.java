package com.abc.carrygo.common.serde;

import com.google.protobuf.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by plin on 10/20/16.
 */
public class ProtobufSerde {
    public static final Logger log = LoggerFactory.getLogger(ProtobufSerde.class);

    public static byte[] toBinary(Message msg) {
        try {
            return msg.toByteArray();
        } catch (Exception e) {
            log.warn("Serialize protobuf to binary failed.", e);
            return null;
        }
    }

    public static <T extends Message> T fromBinary(byte[] bytes, T obj) {
        try {
            Message message = obj.newBuilderForType().mergeFrom(bytes).buildPartial();
            return (T) message;
        } catch (Exception e) {
            log.warn("Deserialize protobuf from binary failed.", e);
            return null;
        }
    }
}
