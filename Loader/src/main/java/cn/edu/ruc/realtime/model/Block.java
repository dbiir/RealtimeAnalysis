package cn.edu.ruc.realtime.model;

import cn.edu.ruc.realtime.utils.ConfigFactory;

import java.util.*;

/**
 * RealTimeAnalysis
 *
 * @author Jelly
 *
 * Block of {@link Batch}es. A {@link Block} consists of serveral {@Link BlockPartition}, which is a clustered collection
 * of {@link Message}s of same partition ordering by timestamp.
 */
public class Block {

    private ConfigFactory configFactory = ConfigFactory.getInstance();
    private int capacity = configFactory.getWriterBlockSize();
    // partition -> List<Batch>
    private HashMap<Integer, List<Batch>> bufferMap = new HashMap<>();
    // store all sorted messages in block
    private List<Message> content = new LinkedList<>();
    // store meta data
    private List<Meta> metas = new LinkedList<>();
    // block level minTimestamp
    private long blockMinTimestamp = 0L;
    // block level maxTimestamp
    private long blockMaxTimestamp = 0L;
    // batch counter
    private int counter = 0;

    // sort messages in batchs, and put into content, fill in metas
    public void construct() {
        // put messages into content
        List<Message> partitionContent = new LinkedList<>();
        for (int key: bufferMap.keySet()) {
            List<Batch> batches = bufferMap.get(key);
            Meta meta = new Meta();
            long minBeginTimestamp = 0L;
            long maxEndTimeStamp = 0L;
            // fill in meta
            for (Batch batch: batches) {
                partitionContent.addAll(batch.getBatchContent());
                // update minBeginTimestamp and maxEndTimestamp.
                long minTime = batch.getBegimTime();
                long maxTime = batch.getEndTime();
                if (minBeginTimestamp == 0L) {
                    minBeginTimestamp = minTime;
                } else if (minBeginTimestamp > minTime) {
                    minBeginTimestamp = minTime;
                }
                if (maxEndTimeStamp < maxTime) {
                    maxEndTimeStamp = maxTime;
                }
            }
            meta.setBeginTime(minBeginTimestamp);
            meta.setEndTime(maxEndTimeStamp);
            meta.setFiberId(key);
            metas.add(meta);
            // sort each message list specified to each fiber.
            partitionContent.sort(
                    new Comparator<Message>() {
                        @Override
                        public int compare(Message m1, Message m2) {
                            return msgCompare(m1, m2);
                        }
                    }
            );
            content.addAll(partitionContent);
            partitionContent.clear();
        }
        blockMinTimestamp = getMinTimestamp();
        blockMaxTimestamp = getMaxTimestamp();
    }

    public int msgCompare(Message m1, Message m2) {
        if (m1.getTimestamp() > m2.getTimestamp()) {
            return 1;
        }
        if (m1.getTimestamp() < m2.getTimestamp()) {
            return -1;
        }
        return 0;
    }

    public void addBatch(Batch batch) {
        int key = batch.getPartition();
        if (bufferMap.get(key) == null) {
            List<Batch> list = new ArrayList<>();
            bufferMap.put(key, list);
        }
        bufferMap.get(key).add(batch);
        counter++;
    }

    public boolean isFull() {
        return counter >= capacity;
    }

    public Set<Integer> getFiberId() {
        return bufferMap.keySet();
    }

    public List<Message> getContent() {
        return content;
    }

    public void clear() {
        content.clear();
        bufferMap.clear();
        metas.clear();
        blockMinTimestamp = 0L;
        blockMaxTimestamp = 0L;
        counter = 0;
    }

    public List<Meta> getMetas() {
        return metas;
    }

    private long getMinTimestamp() {
        long minTime = 0L;
        for (Meta meta: metas) {
            if (minTime == 0L) {
                minTime = meta.getBeginTime();
            } else if (minTime > meta.getBeginTime()) {
                minTime = meta.getBeginTime();
            }
        }
        return minTime;
    }

    private long getMaxTimestamp() {
        long maxTime = 0L;
        for (Meta meta: metas) {
            if (maxTime < meta.getEndTime()) {
                maxTime = meta.getEndTime();
            }
        }
        return maxTime;
    }

    public long getBlockMinTimestamp() {
        return blockMinTimestamp;
    }

    public long getBlockMaxTimestamp() {
        return blockMaxTimestamp;
    }
}
