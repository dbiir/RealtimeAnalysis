package cn.edu.ruc.realtime.threads;

import cn.edu.ruc.realtime.model.Batch;
import cn.edu.ruc.realtime.model.Message;
import cn.edu.ruc.realtime.utils.ConfigFactory;
import cn.edu.ruc.realtime.utils.Log;
import cn.edu.ruc.realtime.utils.LogFactory;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author Jelly
 *
 * Manager Thread. Pulling from Kafka and write to HDFS. Serve specific Kafka topic.
 * Manage {@link SimpleLoaderThread} and {@link SimpleWriterThread}.
 * {@link SimpleLoaderThread} is responsible for pulling messages from Kafka broker, and consupulate them {@link Batch},
 * then write into {@link BlockingQueue}.
 * {@link SimpleWriterThread} is responsible for reading from {@link BlockingQueue}, and write out.
 */
public class ThreadManager {
    private static int threadNum = Runtime.getRuntime().availableProcessors() * 2;
    private int partitionNum;
    private List<Integer> partitionIds;
    private String topic;
    private ExecutorService executor;

    private ConfigFactory configFactory = ConfigFactory.getInstance();
    private Log systemLogger = LogFactory.getInstance().getSystemLogger();
    private int blockingQueueSize = configFactory.getThreadQueueSize();
    private int writerThreadNum = configFactory.getWriterThreadNum();

    private final List<LoaderThread> loaderMap = new LinkedList<>();
    private final List<WriterThread> writerMap = new LinkedList<>();
    private final BlockingQueue<Batch> queue = new ArrayBlockingQueue(blockingQueueSize);

    public ThreadManager(String topic, List<Integer> partitionIds) {
        this.topic = topic;
        this.partitionIds = partitionIds;
        partitionNum = partitionIds.size();
        executor = Executors.newFixedThreadPool(threadNum);
    }

    public void execute() {
        for (int i = 0; i < writerThreadNum; i++) {
            System.out.println();
            SimpleWriterThread writer = new SimpleWriterThread("Writer-" + topic + "-" + i, queue);
//            WriterThread writer = new ConsoleWriterThread(queue);
            writerMap.add(writer);
            executor.execute(writer);
        }

        for (int i = 0; i < partitionNum; i++) {
            systemLogger.info("Loader Thread Partition: " + i + " started.");
            LoaderThread loader = new SimpleLoaderThread(topic, partitionIds.get(i), queue);
            loaderMap.add(loader);
            executor.execute(loader);
        }
    }

    public void shutdownAll() {
        shutdownLoader();
        shutdownWriter();
        executor.shutdown();
    }

    public void shutdownLoader() {
        for (LoaderThread t: loaderMap) {
            t.setReadyToStop();
        }
    }

    public void shutdownWriter() {
        for (WriterThread t: writerMap) {
            t.setReadyToStop();
        }
    }
}
