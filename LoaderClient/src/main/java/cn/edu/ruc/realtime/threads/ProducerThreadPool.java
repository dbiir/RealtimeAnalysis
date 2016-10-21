package cn.edu.ruc.realtime.threads;

import cn.edu.ruc.realtime.model.Message;
import cn.edu.ruc.realtime.utils.ConfigFactory;
import cn.edu.ruc.realtime.utils.Log;
import cn.edu.ruc.realtime.utils.LogFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

/**
 * @author Jelly
 *
 * Thread pool for {@link ProducerThread}. Send messages into Kafka
 */
public class ProducerThreadPool {
    // default thread pool size is equal to physical thread num: 2 * (num of processors)
    private int threadPoolSize = Runtime.getRuntime().availableProcessors() * 2;
    private ConfigFactory config = ConfigFactory.getInstance();
    private ExecutorService executor;
    // blocking array queue
    private ArrayBlockingQueue<Message> queue;
    // kafka topic
    private String topic;
    // consumer thread list
    private List<ProducerThread> consumerList;
    // default blocking array queue size
    private int queueSize = 1000;
    // default logger
    private Log systemLogger = LogFactory.getInstance().getSystemLogger();

    public ProducerThreadPool(String topic) {
        // if customized thread pool size is larger, set to customized one, else stick to default
        if (config.getThreadPoolSize() > threadPoolSize)
            threadPoolSize = config.getThreadPoolSize();
        // if customized blocking queue size is specified, set to customized one, else stick to default
        if (config.getBlockingPoolSize() != 0)
            queueSize = config.getBlockingPoolSize();
        executor = Executors.newFixedThreadPool(threadPoolSize);
        this.topic = topic;
        queue = new ArrayBlockingQueue(queueSize);
        consumerList = new ArrayList<>(threadPoolSize);
    }

    public void execute() {
        systemLogger.info("Execute threads num: " + threadPoolSize);
        for (ProducerThread producerThread : consumerList) {
            executor.execute(producerThread);
            systemLogger.info("Launch thread " + producerThread.getThreadName());
        }
    }

    public void shutdown() {
        for (ProducerThread producerThread : consumerList) {
            producerThread.setReadyToStop();
        }
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

    public void addConsumer(ProducerThread consumer) {
        consumerList.add(consumer);
    }

    public BlockingQueue<Message> getQueue() {
        return this.queue;
    }
}
