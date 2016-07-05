package cn.edu.ruc.realtime.threads;

import cn.edu.ruc.realtime.utils.ConfigFactory;
import cn.edu.ruc.realtime.utils.Log;
import cn.edu.ruc.realtime.utils.LogFactory;
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
    private final ConfigFactory configFactory = ConfigFactory.getInstance();
    private final String basePath = configFactory.getWriterFilePath();
    private final long blockSize = configFactory.getWriterBlockSize();
    private final float fullFactor = configFactory.getWriterFullFactor();
    private Log systemLogger = LogFactory.getInstance().getSystemLogger();
    private String threadName;
    private BlockingQueue<T> queue;
    private Queue<T> writerQueue = new LinkedList();
    private Writer writer;

    public WriterThread(String threadName, BlockingQueue queue) {
        this.threadName = threadName;
        this.queue = queue;
    }

    @Override
    public void run() {
        systemLogger.info(getName() + ": started");
        writer = new FileWriter(basePath);
        while (!Thread.interrupted()) {
            try {
                if (isReadyToWrite()) {
                    systemLogger.info(getName() + ": Ready to write");
                    writer.write(writerQueue);
                }
                T message = queue.take();
                writerQueue.add(message);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public boolean isReadyToWrite() {
        if (this.writerQueue.size() > (blockSize * fullFactor)) {
            return true;
        }
        return false;
    }

    public String getName() {
        return this.threadName;
    }

    public void shutdown() {
        this.shutdown();
    }

    public String toString() {
        return this.getName();
    }
}
