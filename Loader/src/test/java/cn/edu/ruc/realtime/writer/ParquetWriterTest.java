package cn.edu.ruc.realtime.writer;


import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/**
 * Created by Jelly on 6/29/16.
 */
public class ParquetWriterTest {
    private static BlockingQueue queue = new ArrayBlockingQueue(1000);

    public static void main(String[] args) {
        ParquetWriterThread thread = new ParquetWriterThread(queue);
        thread.run();
    }
}
