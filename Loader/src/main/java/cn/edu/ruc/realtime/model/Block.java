package cn.edu.ruc.realtime.model;

import cn.edu.ruc.realtime.utils.ConfigFactory;
import cn.edu.ruc.realtime.utils.Log;
import cn.edu.ruc.realtime.utils.LogFactory;

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
    private HashMap<Long, List<Message>> bufferMap = new HashMap<>();
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
//        long before = System.currentTimeMillis();
        // put messages into content
        for (long key: bufferMap.keySet()) {
            List<Message> messages = bufferMap.get(key);
            Meta meta = new Meta();
            long minBeginTimestamp = messages.get(0).getTimestamp();
            long maxEndTimeStamp = messages.get(0).getTimestamp();
            // fill in meta
            for (Message message: messages) {
                // update minBeginTimestamp and maxEndTimestamp.
                long timestamp = message.getTimestamp();
                if (timestamp > maxEndTimeStamp) {
                    maxEndTimeStamp = timestamp;
                }
                if (timestamp < minBeginTimestamp) {
                    minBeginTimestamp = timestamp;
                }
            }
            meta.setBeginTime(minBeginTimestamp);
            meta.setEndTime(maxEndTimeStamp);
            meta.setFiberId(key);
            metas.add(meta);
            // sort messages of the same fiber by timestamp.
            messages.sort(
                    new Comparator<Message>() {
                        @Override
                        public int compare(Message m1, Message m2) {
                            return msgCompare(m1, m2);
                        }
                    }
            );
            content.addAll(messages);
            messages.clear();
        }
        blockMinTimestamp = getMinTimestamp();
        blockMaxTimestamp = getMaxTimestamp();
//        long end = System.currentTimeMillis();
//        System.out.println("Construction cost: " + (end - before) + " ms");
//        systemLogger.info("Construction cost: " + (end - before) + " ms");
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
        for (Message msg : batch.getBatchContent()) {
            long key = msg.getKey();
            if (!bufferMap.containsKey(key)) {
                bufferMap.put(key, new ArrayList<Message>());
            }
            bufferMap.get(key).add(msg);
        }
        counter++;
    }

    public boolean isFull() {
        return counter >= capacity;
    }

    public Set<Long> getFiberId() {
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
