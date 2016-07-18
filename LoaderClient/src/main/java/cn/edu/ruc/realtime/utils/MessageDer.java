package cn.edu.ruc.realtime.utils;

import cn.edu.ruc.realtime.model.Message;
import kafka.serializer.Decoder;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

/**
 * RealTimeAnalysis
 *
 * @author Jelly
 */
public class MessageDer implements Deserializer<Message> {
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public Message deserialize(String topic, byte[] data) {
        return Message.fromBytes(data);
    }

    @Override
    public void close() {

    }
}
