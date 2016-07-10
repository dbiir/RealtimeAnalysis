package cn.edu.ruc.realtime.model;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created by Jelly on 7/10/16.
 * Batch used as loader thread buffer.
 */
public class Batch<T> {
    private long lastOffset;
    private int partition;
    private List<T> batchContent;
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

    public int getPartition() {
        return this.partition;
    }

    public void addMsg(T msg, long offset) {
        if (offset > getLastOffset()) {
            this.lastOffset = offset;
        }
        batchContent.add(msg);
    }

    public Iterator<T> getIterator() {
        return batchContent.iterator();
    }

    public void clear() {
        batchContent.clear();
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (T e: batchContent) {
            sb.append(e);
        }
        sb.append("-").append(lastOffset);
        return sb.toString();
    }
}
