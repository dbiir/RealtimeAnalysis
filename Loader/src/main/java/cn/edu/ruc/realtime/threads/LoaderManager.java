package cn.edu.ruc.realtime.threads;

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
    private final HashMap<String, LoaderThread> loaderMap = new HashMap<>();
    private final BlockingQueue queue = new ArrayBlockingQueue(10);

    public LoaderManager(String topic, int partitionNum) {
        this.topic = topic;
        this.partitionNum = partitionNum;
        executor = Executors.newFixedThreadPool(this.partitionNum);
    }

    public void execute() {
        int tCounter = 0;
        while (tCounter < partitionNum) {
            LoaderThread loader = new LoaderThread(topic, tCounter, queue);
            loaderMap.put(loader.getName(), loader);
            executor.execute(loader);
            tCounter++;
        }
        // TODO Multi writer thread
        WriterThread writer = new WriterThread("Writer", queue);
        writer.run();
    }

    public void shutdownAll() {
        for (LoaderThread loader: loaderMap.values()) {
            loader.shutdown();
        }
        executor.shutdown();
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
