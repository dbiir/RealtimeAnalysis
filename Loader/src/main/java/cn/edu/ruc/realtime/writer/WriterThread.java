package cn.edu.ruc.realtime.writer;

import java.util.concurrent.BlockingQueue;

/**
 * Created by Jelly on 6/29/16.
 */
public class WriterThread implements Runnable {

    private BlockingQueue queue;

    public WriterThread(BlockingQueue queue) {
        this.queue = queue;
    }

    @Override
    public void run() {
        while (true) {
//            if (queue.remainingCapacity() < )
        }
    }
}
