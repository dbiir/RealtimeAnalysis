package cn.edu.ruc.realtime.threads;

import cn.edu.ruc.realtime.writer.FileWriter;
import cn.edu.ruc.realtime.writer.Writer;

import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;

/**
 * Created by Jelly on 6/29/16.
 * Writer Thread. Write data to HDFS.
 */
public class WriterThread<T> implements Runnable {

    // TODO should read from config file
    private final static int blockSize = 100;
    private final static float fullFactor = 0.95f;
    private String threadName;
    private BlockingQueue<T> queue;
    private Queue<T> threadQueue = new LinkedList();
    private Writer writer = new FileWriter("/Users/Jelly/Developer/RealTimeAnalysis/lineorders_3_result");

    public WriterThread(String threadName, BlockingQueue queue) {
        this.threadName = threadName;
        this.queue = queue;
    }

    @Override
    public void run() {
        System.out.println(getThreadName() + " started");
        while (!Thread.interrupted()) {
            try {
                if (isReadyToWrite()) {
                    System.out.println(getThreadName() + ": Ready to write");
                    writer.write(threadQueue);
                }
                T message = queue.take();
                threadQueue.add(message);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public boolean isReadyToWrite() {
        if (this.threadQueue.size() > (blockSize * fullFactor)) {
            return true;
        }
        return false;
    }

    public String getThreadName() {
        return this.threadName;
    }
}
