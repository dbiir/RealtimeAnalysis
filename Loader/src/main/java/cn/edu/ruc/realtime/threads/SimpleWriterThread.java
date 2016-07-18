package cn.edu.ruc.realtime.threads;

import cn.edu.ruc.realtime.model.Batch;
import cn.edu.ruc.realtime.model.Block;
import cn.edu.ruc.realtime.model.Meta;
import cn.edu.ruc.realtime.utils.*;
import cn.edu.ruc.realtime.writer.FileWriter;
import cn.edu.ruc.realtime.writer.Writer;

import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by Jelly on 6/29/16.
 * Writer Thread. Write data to HDFS.
 */
public class SimpleWriterThread extends WriterThread {

    private Log systemLogger = LogFactory.getInstance().getSystemLogger();
    private String threadName;
    private BlockingQueue<Batch> queue;
    private Block block = new Block();
    private Writer writer;
    private HashMap<Integer, Long> offsetMap = new HashMap<>();
//    private DBConnection dbConnection = new PostgresConnection();

    // isReadyToStop signal
    private AtomicBoolean isReadyToStop = new AtomicBoolean(false);

    public SimpleWriterThread(String threadName, BlockingQueue queue) {
        this.threadName = threadName;
        this.queue = queue;
    }

    @Override
    public void run() {
        //TODO Add time limit
        systemLogger.info(getName() + ": started");
        writer = new FileWriter();
        while (true) {
            if (readyToStop() && queue.isEmpty()) {
                break;
            }
            try {
                if (block.isFull()){
                    systemLogger.info(getName() + ": Ready to write");
                    // construct block.
                    block.construct();
                    // write to file
                    String filename = writer.write(block.getFiberId(),
                            block.getContent(),
                            block.getBlockMinTimestamp(),
                            block.getBlockMaxTimestamp());
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
                        systemLogger.info("Write success, clear block");
                        block.clear();
                    } else {
                        // TODO file write failed
                    }
                }
                Batch msgBatch = queue.take();
                systemLogger.info("SimpleWriterThread add batch");
                block.addBatch(msgBatch);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
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
        // commit offsets of whole block to storage
//        dbConnection.commitPartitionOffsets(commitMap);
    }

    /**
     * Commit to meta server
     * fiberId, beginTime, endTime, filename
     * */
    public void commitMeta(int fiberId, long beginTime, long endTime, String filename) {
        System.out.print("Meta: " + fiberId + "-" + beginTime + "-" + endTime + "-" + filename);
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
        return isReadyToStop.get();
    }

    public String toString() {
        return this.getName();
    }
}
