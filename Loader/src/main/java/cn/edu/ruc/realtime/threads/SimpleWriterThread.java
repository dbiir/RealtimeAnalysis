package cn.edu.ruc.realtime.threads;

import cn.edu.ruc.realtime.model.Batch;
import cn.edu.ruc.realtime.model.Block;
import cn.edu.ruc.realtime.model.Meta;
import cn.edu.ruc.realtime.utils.*;
import cn.edu.ruc.realtime.writer.FileWriter;
import cn.edu.ruc.realtime.writer.HadoopWriter;
import cn.edu.ruc.realtime.writer.ParquetHadoopWriter;
import cn.edu.ruc.realtime.writer.Writer;

import java.sql.Timestamp;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by Jelly on 6/29/16.
 * Writer Thread. Write data to HDFS.
 */
public class SimpleWriterThread extends WriterThread {

    private Log systemLogger = LogFactory.getInstance().getSystemLogger();
    private String threadName;
    private BlockingQueue<Batch> queue;
    private Writer writer;
    private HashMap<Integer, Long> offsetMap = new HashMap<>();
    private DBConnection dbConnection = new PostgresConnection();

    // isReadyToStop signal
    private AtomicBoolean isReadyToStop = new AtomicBoolean(false);
    private AtomicLong counter = new AtomicLong(0L);

    public SimpleWriterThread(String threadName, BlockingQueue queue) {
        this.threadName = threadName;
        this.queue = queue;
    }

    @Override
    public void run() {
        Block block = new Block();
        //TODO Add time limit
        systemLogger.info(getName() + ": started");
//        writer = new FileWriter();
//        writer = new HadoopWriter();
        writer = new ParquetHadoopWriter();
        while (true) {
            if (readyToStop()) {
                writerBlock(block);
                break;
            }
            try {
                if (block.isFull()){
                    systemLogger.info(getName() + "Current time: " + System.currentTimeMillis() + ". Ready to write");
                    writerBlock(block);
                }
                Batch msgBatch = queue.take();
                counter.getAndIncrement();
//                systemLogger.info("SimpleWriterThread add batch");
//                systemLogger.info("Writer thread, block counter: " + counter.get());
                block.addBatch(msgBatch);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public void writerBlock(Block block) {
        block.construct();
        // write to file
        long before = System.currentTimeMillis();
        String filename = writer.write(block.getFiberId(),
                block.getContent(),
                block.getBlockMinTimestamp(),
                block.getBlockMaxTimestamp());
        long end = System.currentTimeMillis();
        systemLogger.info("Current time: " + System.currentTimeMillis() + "Write cost: " + (end - before) + " ms");
        // write succeeds, commit meta
        if (filename != null) {
            for (Meta meta: block.getMetas()) {
                commitMeta(meta.getFiberId(),
                        meta.getBeginTime(),
                        meta.getEndTime(),
                        filename);
            }
//                        commitOffset(writerQueue);
            // clear block content
            block.clear();
            long allEnd = System.currentTimeMillis();
            systemLogger.info("Current time: " + System.currentTimeMillis() + "Commit meta cost: " + (allEnd - end) + " ms");
            System.out.println("Done block");
        } else {
            // TODO file write failed
        }
    }

    /**
     * Commit offset to storage
     * */
    public void commitOffset(Queue<Batch> queue) {
        Batch batch;
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
    }

    /**
     * Commit to meta server
     * fiberId, beginTime, endTime, filename
     * */
    public void commitMeta(int fiberId, long beginTime, long endTime, String filename) {
        dbConnection.commitMetaRecord(fiberId, filename, new Timestamp(beginTime), new Timestamp(endTime));
//        System.out.println("Meta: " + fiberId + "-" + beginTime + "-" + endTime + "-" + filename);
    }

    public String getName() {
        return this.threadName;
    }

    @Override
    public void setReadyToStop() {
        isReadyToStop.set(true);
    }

    @Override
    public boolean readyToStop() {
        return isReadyToStop.get() && queue.isEmpty();
    }

    public String toString() {
        return this.getName();
    }
}
