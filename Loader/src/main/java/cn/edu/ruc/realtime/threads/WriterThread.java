package cn.edu.ruc.realtime.threads;

import cn.edu.ruc.realtime.model.Batch;
import cn.edu.ruc.realtime.utils.*;
import cn.edu.ruc.realtime.writer.FileWriter;
import cn.edu.ruc.realtime.writer.Writer;

import java.util.*;
import java.util.concurrent.BlockingQueue;

/**
 * Created by Jelly on 6/29/16.
 * Writer Thread. Write data to HDFS.
 */
public class WriterThread<T> implements Runnable {

    private final ConfigFactory configFactory = ConfigFactory.getInstance();
    private final String basePath = configFactory.getWriterFilePath();
    private final long blockSize = configFactory.getWriterBlockSize();
    private final float fullFactor = configFactory.getWriterFullFactor();
    private Log systemLogger = LogFactory.getInstance().getSystemLogger();
    private String threadName;
    private BlockingQueue<Batch<T>> queue;
    private Queue<Batch<T>> writerQueue = new LinkedList();
    private Writer writer;
    private HashMap<Integer, Long> offsetMap = new HashMap<>();
    private DBConnection dbConnection = new PostgresConnection();

    public WriterThread(String threadName, BlockingQueue queue) {
        this.threadName = threadName;
        this.queue = queue;
    }

    @Override
    public void run() {
        //TODO Add time limit
        systemLogger.info(getName() + ": started");
        writer = new FileWriter(basePath);
        while (!Thread.interrupted()) {
            try {
                if (isReadyToWrite()) {
                    systemLogger.info(getName() + ": Ready to write");
                    // write succeeds, store offset to db
                    if (writer.write(writerQueue)) {
                        commitOffset(writerQueue);
                    }
                }
                Batch<T> msgBatch = queue.take();
                writerQueue.add(msgBatch);
            } catch (InterruptedException e) {
                e.printStackTrace();
            } finally {
//                writer.write(writerQueue);
            }
        }
    }

    public boolean isReadyToWrite() {
        if (this.writerQueue.size() >= (blockSize * fullFactor)) {
            return true;
        }
        return false;
    }

    /**
     * Commit offset to storage
     * */
    public void commitOffset(Queue<Batch<T>> queue) {
        Batch<T> batch;
        HashMap<Integer, Long> commitMap = new HashMap<>();
        while (queue.peek() != null) {
            batch = queue.poll();
            if (offsetMap.containsKey(batch.getPartition())) {
                int key = batch.getPartition();
                long value = batch.getLastOffset();
                if (value > offsetMap.get(key)) {
                    offsetMap.put(key, value);
                }
                commitMap.put(key, value);
            } else {
                int key = batch.getPartition();
                long value = batch.getLastOffset();
                offsetMap.put(key, value);
                commitMap.put(key, value);
            }
        }
        // commit offsets of whole block to storage
        dbConnection.commitPartitionOffsets(commitMap);
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
