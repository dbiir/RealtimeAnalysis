package cn.edu.ruc.realtime.model;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * @author Jelly
 * Batch used as loader thread buffer.
 * Not thread safe
 */
public class Batch {
    private long firstOffset = 0L;
    private long lastOffset = 0L;
    private long begimTime = 0L;
    private long endTime = 0L;
    private int partition;
    private List<Message> batchContent;
    private int capacity;

    public Batch(int capacity, int partition) {
        this.capacity = capacity;
        this.partition = partition;
        batchContent = new ArrayList<>(capacity);
    }

    public boolean isFull() {
        if (batchContent.size() >= capacity) {
            return true;
        }
        return false;
    }

    public long getLastOffset() {
        return this.lastOffset;
    }

    public long getFirstOffset() {
        return this.firstOffset;
    }

    public int getPartition() {
        return this.partition;
    }

    public void addMsg(Message msg, long offset) {
        long timestamp = msg.getTimestamp();
        if (lastOffset < offset) {
            lastOffset = offset;
        }
        if (firstOffset == 0L) {
            firstOffset = offset;
        } else if (firstOffset > offset) {
            firstOffset = offset;
        }
        if (begimTime == 0L) {
            begimTime = timestamp;
        } else if (begimTime > timestamp) {
            begimTime = timestamp;
        }
        if (endTime < timestamp) {
            endTime = timestamp;
        }
        batchContent.add(msg);
    }

    public Iterator<Message> getIterator() {
        return batchContent.iterator();
    }

    public List<Message> getBatchContent() {
        return batchContent;
    }

    public long getBegimTime() {
        return begimTime;
    }

    public long getEndTime() {
        return endTime;
    }

    public void clear() {
        batchContent.clear();
        firstOffset = 0L;
        lastOffset = 0L;
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(firstOffset).append("<");
        for (Message e: batchContent) {
            sb.append(e.toString()).append(" ");
        }
        sb.append(">").append(lastOffset);
        return sb.toString();
    }

    public int getSize() {
        return batchContent.size();
    }

    public boolean isEmpty() {
        return batchContent.size() > 0;
    }
}
