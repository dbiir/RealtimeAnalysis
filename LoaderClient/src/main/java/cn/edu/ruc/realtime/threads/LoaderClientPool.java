package cn.edu.ruc.realtime.threads;

import cn.edu.ruc.realtime.model.Message;
import cn.edu.ruc.realtime.utils.ConfigFactory;
import cn.edu.ruc.realtime.utils.Log;
import cn.edu.ruc.realtime.utils.LogFactory;

import java.util.concurrent.*;

/**
 * Created by Jelly on 6/27/16.
 */
public class LoaderClientPool {
    // default thread pool size is equal to physical thread num: 2 * (num of processors)
    private int threadPoolSize = Runtime.getRuntime().availableProcessors() * 2;
    private ConfigFactory config = ConfigFactory.getInstance();
    private ExecutorService executor;
    // blocking array queue
    private ArrayBlockingQueue<Message> queue;
    private String topic;
    // default blocking array queue size
    private int queueSize = 1000;
    // default logger
    private Log systemLogger = LogFactory.getInstance().getSystemLogger();

    public LoaderClientPool(String topic) {
        // if customized thread pool size is larger, set to customized one, else stick to default
        if (config.getThreadPoolSize() > threadPoolSize)
            threadPoolSize = config.getThreadPoolSize();
        // if customized thread queue size is specified, set to customized one, else stick to default
        if (config.getThreadQueueSize() != 0)
            queueSize = config.getThreadQueueSize();
        executor = Executors.newFixedThreadPool(threadPoolSize);
        this.topic = topic;
        queue = new ArrayBlockingQueue(queueSize);
    }

    public void execute() {
        System.out.println("Execute threads num: " + threadPoolSize);
        systemLogger.info("Execute threads num: " + threadPoolSize);
        int tCounter = 0;
        while (tCounter < threadPoolSize) {
            String loaderName = "thread:" + topic + "-" + tCounter;
            executor.execute(new LoaderClientThread<>(topic, loaderName, queue));
            systemLogger.info("Launch thread " + loaderName);
            tCounter++;
        }
    }

    public void shutdown() {
        executor.shutdown();
        systemLogger.info("Executor shutdown");
    }

    public void shutdownNow() {
        executor.shutdownNow();
    }

    public boolean isTerminated() {
        return executor.isTerminated();
    }

    public void put(Message message) {
        try {
            queue.put(message);
        } catch (InterruptedException e) {
            systemLogger.exception(e);
        }
    }
}
