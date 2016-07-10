package cn.edu.ruc.realtime.client;

import cn.edu.ruc.realtime.model.Message;
import cn.edu.ruc.realtime.utils.ConfigFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by Jelly on 6/28/16.
 */
public class LoaderClientTest {

    public static void main(String[] args) throws IOException {
        if (args.length != 1) {
            System.out.println("Usage: java -jar loaderClient propertiesFilePath");
            System.exit(1);
        }

        System.out.println(args[0]);

        // load config
        ConfigFactory configFactory = ConfigFactory.getInstance(args[0]);

//        BufferedReader reader = new BufferedReader(new FileReader("/home/kafka/lineorders000"));
        List<Message> messageList = new LinkedList<>();

        String topic = "test07051224";
        String line;

        LoaderClient client = new LoaderClient(topic);
//
//        System.out.println("Start reading from file...");
//        while ((line = reader.readLine()) != null) {
//            String key = line.split("|")[0];
//            Message<String, String> message = new Message<>(key, line);
//            messageList.add(message);
//        }

        for (int i = 0; i < 1000; i++) {
            Message<String, String> message = new Message<>(String.valueOf(i), "test"+i);
            messageList.add(message);
        }
        long before = System.currentTimeMillis();
        System.out.println("Start loading...");
        client.sendMessages(messageList);

        System.out.println("Loading ended. Cost: " + (System.currentTimeMillis() - before) + "ms");
        client.shutdown();
    }
}
