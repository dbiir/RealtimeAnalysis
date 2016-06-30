package cn.edu.ruc.realtime.writer;

import java.util.Queue;

/**
 * Created by Jelly on 6/30/16.
 * Parquet Writer
 */
public class ParquetWriter implements Writer {

    @Override
    public void write(Queue queue) {
        while (queue.peek() != null) {
            System.out.println(queue.poll());
        }
    }
}
