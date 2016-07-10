package cn.edu.ruc.realtime.threads;

import cn.edu.ruc.realtime.model.Batch;
import cn.edu.ruc.realtime.utils.ConfigFactory;

import java.util.HashMap;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by Jelly on 6/29/16.
 * Manager Process. Pulling from Kafka and write to HDFS.
 */
public class LoaderManager {
    private int partitionNum;
    private String topic;
    private ExecutorService executor;
    private ConfigFactory configFactory = ConfigFactory.getInstance();
    private int blockingQueueSize = configFactory.getBlockingQueueSize();
    private int writerThreadNum = configFactory.getWriterThreadNum();
    private final HashMap<String, LoaderThread> loaderMap = new HashMap<>();
    private final HashMap<String, WriterThread> writerMap = new HashMap<>();
    private final BlockingQueue<Batch> queue = new ArrayBlockingQueue(blockingQueueSize);

    public LoaderManager(String topic, int partitionNum) {
        this.topic = topic;
        this.partitionNum = partitionNum;
        executor = Executors.newCachedThreadPool();
    }

    public void execute() {
        for (int i = 0; i < partitionNum; i++) {
            LoaderThread loader = new LoaderThread(topic, i, queue);
            loaderMap.put(loader.getName(), loader);
            executor.execute(loader);
        }
        for (int i = 0; i < writerThreadNum; i++) {
            WriterThread writer = new WriterThread("Writer-" + topic + "-" + i, queue);
            writerMap.put(writer.getName(), writer);
            executor.execute(writer);
        }
    }


    public void shutdown() {
        executor.shutdown();
    }

    public void shutdownNow() {
        executor.shutdownNow();
    }

    public void shutdownAll() {
        for (WriterThread t: writerMap.values()) {
            t.shutdown();
        }
        for (LoaderThread t: loaderMap.values()) {
            t.shutdown();
        }
    }

    public void shutdownLoader(String loaderName) {
        LoaderThread loader = loaderMap.get(loaderName);
        if (loader != null)
            loader.shutdown();
    }

    public void pauseLoader(String loaderName) {
        LoaderThread loader = loaderMap.get(loaderName);
        if (loader != null)
            loader.pause();
    }

    public void wakeUpLoader(String loaderName) {
        LoaderThread loader = loaderMap.get(loaderName);
        if (loader != null)
            loader.wakeUp();
    }
}
