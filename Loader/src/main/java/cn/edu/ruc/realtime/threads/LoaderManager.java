package cn.edu.ruc.realtime.threads;

import cn.edu.ruc.realtime.writer.WriterThread;

import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by Jelly on 6/29/16.
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
        WriterThread writer = new WriterThread(queue);
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
