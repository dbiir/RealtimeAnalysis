package cn.edu.ruc.realtime.writer;

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.concurrent.BlockingQueue;

/**
 * Created by Jelly on 6/29/16.
 * Writer Thread. Write data to HDFS.
 */
public class WriterThread implements Runnable {

    private String threadName;
//    private ConfigFactory config = ConfigFactory.getInstance();
    private BlockingQueue queue;
//    private int capacity=Integer.parseInt(config.getProps("queue.capacity"));
    private int capacity = 10;
    private double exhaustFactor = 0.1;
//    private double exhaustFactor = Double.parseDouble(config.getProps("queue.factor"));
    private Collection collection = new LinkedList();

    public WriterThread(String threadName, BlockingQueue queue) {
        this.threadName = threadName;
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
                    System.out.println(this.threadName + "> " + iterator.next().toString());
                }
            }
        }
    }
}
