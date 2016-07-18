package cn.edu.ruc.realtime.model;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * @author Jelly
 * Batch used as loader thread buffer.
 */
public class Batch {
    private long firstOffset = 0L;
    private long lastOffset = 0L;
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
        if (offset > getLastOffset()) {
            this.lastOffset = offset;
        }
        if (offset < getFirstOffset()) {
            this.firstOffset = offset;
        }
        batchContent.add(msg);
    }

    public Iterator<Message> getIterator() {
        return batchContent.iterator();
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
}
