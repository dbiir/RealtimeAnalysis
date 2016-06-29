package cn.edu.ruc.realtime.writer;

import cn.edu.ruc.realtime.utils.ConfigFactory;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.log4j.net.SyslogAppender;

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.concurrent.BlockingQueue;

/**
 * Created by Jelly on 6/29/16.
 */
public class WriterThread implements Runnable {

//    private ConfigFactory config = ConfigFactory.getInstance();
    private BlockingQueue queue;
//    private int capacity=Integer.parseInt(config.getProps("queue.capacity"));
    private int capacity = 10;
    private double exhaustFactor = 0.1;
//    private double exhaustFactor = Double.parseDouble(config.getProps("queue.factor"));
    private Collection collection = new LinkedList();

    public WriterThread(BlockingQueue queue) {
        this.queue = queue;
    }

    @Override
    public void run() {
        System.out.println("Writer started");
        while (true) {
//            System.out.println(queue.remainingCapacity());
//            System.out.println(Math.ceil(capacity*exhaustFactor));
            if (queue.remainingCapacity() < Math.ceil(capacity*exhaustFactor)) {
                System.out.println("Write out");
                queue.drainTo(collection, capacity);
                Iterator iterator = collection.iterator();
                while (iterator.hasNext()) {
                    System.out.println("Writer> " + iterator.next().toString());
                }
            }
        }
    }
}
