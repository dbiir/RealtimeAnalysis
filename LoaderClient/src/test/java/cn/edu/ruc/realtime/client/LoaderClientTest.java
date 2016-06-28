package cn.edu.ruc.realtime.client;

import cn.edu.ruc.realtime.model.Message;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Jelly on 6/28/16.
 */
public class LoaderClientTest {

    @Test
    public void vTest() {
        String topic = "test";
        String schema = "test";

        LoaderClient client = new LoaderClient(topic, schema);

        for (int i = 0; i < 50; i++) {
            Message<String, String> message = new Message<>(String.valueOf(i), "test" + i);
            client.sendMessage(message);
        }

        List<Message> messages = new ArrayList<>();
        for (int i = 50; i < 100; i++) {
            Message<String, String> message = new Message<>(String.valueOf(i), "test" + i);
            messages.add(message);
        }
        client.sendMessages(messages);

        client.shutdown();
    }
}
