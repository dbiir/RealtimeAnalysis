package cn.edu.ruc.realtime.client;

import cn.edu.ruc.realtime.model.Message;
import cn.edu.ruc.realtime.utils.ConfigFactory;
import cn.edu.ruc.realtime.utils.Log;
import cn.edu.ruc.realtime.utils.LogFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

/**
 * RealTimeAnalysis
 * @author guodong
 */
public class LoaderClientTest {

    public static void main(String[] args) throws IOException {
        if (args.length != 3) {
            System.out.println("Usage: java -jar loaderClient propertiesFilePath input topic");
            System.exit(1);
        }

        System.out.println(args[0]);
        String props = args[0];
        String input = args[1];
        String topic = args[2];

        // load config
//        ConfigFactory configFactory = ConfigFactory.getInstance(props);
        Log userLogger = LogFactory.getInstance().getLogger("user.log");

        BufferedReader reader = new BufferedReader(new FileReader(input));
        List<Message> messageList = new LinkedList<>();

        String line;

        LoaderClient client = new LoaderClient(topic, props);

        System.out.println("Start reading from file...");
        while ((line = reader.readLine()) != null) {
            String key = line.split("\\|")[0];
            Message message = new Message(Long.valueOf(key), line);
            message.setTimestamp(System.currentTimeMillis());
            messageList.add(message);
        }

//        int base = 0;
//        for (int i = base; i < 6010; i++) {
//            Message message = new Message(Long.valueOf(i), "test|test|"+i);
//            long time = (long) Math.ceil(System.currentTimeMillis() * Math.random());
//            message.setTimestamp(time);
//            messageList.add(message);
//        }
        long before = System.currentTimeMillis();
        System.out.println("Loading started. " + before);
        userLogger.info("Loading started. " + before);
        System.out.println("Start loading...");
        userLogger.info("Start loading...");
        client.sendMessages(messageList);
//        System.out.println("Loading ended. Cost: " + (System.currentTimeMillis() - before) + "ms");
        client.shutdown();
    }
}
