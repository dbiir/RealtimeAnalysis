package cn.edu.ruc.realtime.utils;

import cn.edu.ruc.realtime.model.Message;
import kafka.serializer.Encoder;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

/**
 * RealTimeAnalysis
 *
 * @author Jelly
 */
public class MessageSer implements Serializer<Message> {

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public byte[] serialize(String topic, Message data) {
        return data.toBytes();
    }

    @Override
    public void close() {

    }
}
