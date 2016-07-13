package cn.ruc.edu.realtime.benchmark;

import cn.edu.ruc.realtime.client.LoaderClient;
import cn.edu.ruc.realtime.model.Message;
import cn.ruc.edu.realtime.generator.RTDataGen;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

/**
 * @author Jelly
 */
public class RTBench {

    private List<Message> container = new LinkedList<Message>();
    private RTDataGen dataGen = null;
    private String topic;
    private LoaderClient loaderClient;
    private Random random = new Random();

    public RTBench(String dataF, String topic) {
        dataGen = new RTDataGen(dataF);
        this.topic = topic;
        loaderClient = new LoaderClient(topic);
    }

    public void run() {
        container = dataGen.generator();
        Iterator iterator = container.iterator();
        long timeStart = System.currentTimeMillis();
        long counter = 0;
        while (iterator.hasNext()) {
            Message msg = (Message) iterator.next();
            msg.setTimestamp(System.currentTimeMillis());
            loaderClient.sendMessage(msg);
//            if (counter < 1000) {
//                System.out.println(msg.toString());
//            }
            counter++;
        }
        long timeEnd = System.currentTimeMillis();
        loaderClient.shutdown();
        container = null;
        System.out.println("Generate " + counter + " messages. Cost: " + (timeEnd - timeStart) + " ms.");
    }
}
