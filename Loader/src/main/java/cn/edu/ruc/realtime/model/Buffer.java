package cn.edu.ruc.realtime.model;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/**
 * Created by Jelly on 6/29/16.
 */
public class Buffer {
    private int size;
    private int capacity;
    private double exhaustFactor = 0.1;
    private List<BlockingQueue> bufferList;

    public Buffer(int size, int capacity, double exhaustFactor) {
        this.size = size;
        this.capacity = capacity;
        this.exhaustFactor = exhaustFactor;
        bufferList = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            BlockingQueue queue = new ArrayBlockingQueue(capacity);
            bufferList.add(queue);
        }
    }

    public boolean isExhaust(BlockingQueue queue) {
        if (queue.remainingCapacity() > Math.ceil(capacity * exhaustFactor))
            return false;
        return true;
    }

    /**
     * Get an unexhausted blocking queue. Else return null.
     * */
    public BlockingQueue getQueue() {
        for (BlockingQueue queue: bufferList) {
            if (!isExhaust(queue))
                return queue;
        }
        return null;
    }
}
