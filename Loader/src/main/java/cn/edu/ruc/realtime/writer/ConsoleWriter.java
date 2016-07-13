package cn.edu.ruc.realtime.writer;

import java.util.Queue;

/**
 * Created by Jelly on 7/1/16.
 */
public class ConsoleWriter implements Writer {

    @Override
    public boolean write(Queue queue) {
        while (queue.peek() != null) {
            System.out.println(queue.poll());
        }
        return true;
    }
}
