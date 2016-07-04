package cn.edu.ruc.realtime.client;

import cn.edu.ruc.realtime.model.Message;
import cn.edu.ruc.realtime.utils.ConfigFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Jelly on 6/28/16.
 */
public class LoaderClientTest {

    public static void main(String[] args) {
        if (args.length != 1) {
            System.out.println("Usage: java -jar loaderClient propertiesFilePath");
            System.exit(1);
        }

        System.out.println(args[0]);

        ConfigFactory configFactory = ConfigFactory.getInstance(args[0]);

        String topic = "test07040447";

        LoaderClient client = new LoaderClient(topic);

        for (int i = 0; i < 50; i++) {
            Message<String, String> message = new Message<>(String.valueOf(i), "message" + i);
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
