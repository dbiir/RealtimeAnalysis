package cn.edu.ruc.realtime.threads;

import cn.edu.ruc.realtime.model.Message;

import java.util.concurrent.*;

/**
 * Created by Jelly on 6/27/16.
 */
public class LoaderPool {
    private int threadPoolSize = Runtime.getRuntime().availableProcessors() * 2;
//    private ConfigFactory config = ConfigFactory.getInstance();
    private ExecutorService executor;
    private BlockingQueue<Message> queue;
    private String topic;
//    private int queueSize = 1000;

    public LoaderPool(String topic) {
        executor = Executors.newFixedThreadPool(threadPoolSize);
        this.topic = topic;
        // Using ArrayBlockingQueue is more stable than LinkedBlockingQueue, while the latter one has higher throughput
//        queue = new ArrayBlockingQueue<Message>(queueSize);
        queue = new LinkedBlockingQueue<>();
    }

    public void execute() {
        System.out.println("Execute threads num: " + threadPoolSize);
        int tCounter = 0;
        while (tCounter < threadPoolSize) {
            String loaderName = "thread:" + topic + "-" + tCounter;
            executor.execute(new LoaderThread<>(topic, loaderName, queue));
            tCounter++;
        }
    }

    public void shutdown() {
        executor.shutdown();
    }

    public void put(Message message) {
        try {
            queue.put(message);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
