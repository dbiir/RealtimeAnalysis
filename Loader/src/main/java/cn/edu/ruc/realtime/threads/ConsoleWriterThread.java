package cn.edu.ruc.realtime.threads;

import cn.edu.ruc.realtime.model.Batch;
import cn.edu.ruc.realtime.model.Message;

import java.util.Iterator;
import java.util.concurrent.BlockingQueue;

/**
 * RealTimeAnalysis
 * Print result to console. Just for TEST
 *
 * @author Jelly
 */
public class ConsoleWriterThread extends WriterThread {
    private BlockingQueue<Batch> queue;

    public ConsoleWriterThread(BlockingQueue queue) {
        this.queue = queue;
    }

    @Override
    public String getName() {
        return null;
    }

    @Override
    public void setReadyToStop() {

    }

    @Override
    public boolean readyToStop() {
        return false;
    }

    @Override
    public void run() {
        Batch batch;
        Iterator<Message> iterator;
        Message msg;
        int counter = 0;
        while (true) {
            if (readyToStop() && queue.isEmpty()) {
                break;
            }
            System.out.println("Console writer");
            try {
                batch = queue.take();
                counter++;
                System.out.println(counter);
                iterator = batch.getIterator();
                while (iterator.hasNext()) {
                    msg = iterator.next();
                    System.out.println(msg.toString());
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
