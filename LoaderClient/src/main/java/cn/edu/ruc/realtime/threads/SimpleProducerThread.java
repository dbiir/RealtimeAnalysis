package cn.edu.ruc.realtime.threads;

import cn.edu.ruc.realtime.model.Message;
import cn.edu.ruc.realtime.utils.Log;
import cn.edu.ruc.realtime.utils.LogFactory;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;

/**
 * @author Jelly
 *
 * Simple Producer.
 * Get data from blocking queue, and count
 * Just for test
 */
public class SimpleProducerThread extends ProducerThread {
    private String threadName;
    private BlockingQueue<Message> blockingQueue;
    private AtomicLong msgCounter = new AtomicLong(0L);
    private AtomicBoolean isReadyToStop = new AtomicBoolean(false);
    private Log systemLogger = LogFactory.getInstance().getSystemLogger();

    public SimpleProducerThread(String threadName, BlockingQueue<Message> blockingQueue) {
        this.threadName = threadName;
        this.blockingQueue = blockingQueue;
    }

    @Override
    public String getThreadName() {
        return this.threadName;
    }

    @Override
    public void run() {
        long before = System.currentTimeMillis();
        while (true){
            if (readyToStop() && blockingQueue.isEmpty()) {
                System.out.println(getThreadName() + " quit.");
                systemLogger.info(getThreadName() + " quit");
                break;
            }
            try {
                blockingQueue.take();
                msgCounter.getAndIncrement();
            } catch (InterruptedException e) {

            }
        }
        blockingQueue = null;
        long end = System.currentTimeMillis();
        System.out.println("Consumed messages: " + msgCounter.get() + " Cost: " + (end - before) + " ms");
    }

    public void setReadyToStop() {
        isReadyToStop.set(true);
    }

    public boolean readyToStop() {
        return isReadyToStop.get();
    }

    public void close() {
    }
}
