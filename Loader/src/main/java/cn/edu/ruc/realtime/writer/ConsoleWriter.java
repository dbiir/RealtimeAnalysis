package cn.edu.ruc.realtime.writer;

import cn.edu.ruc.realtime.model.Message;

import java.util.List;
import java.util.Queue;
import java.util.Set;

/**
 * Created by Jelly on 7/1/16.
 */
public class ConsoleWriter implements Writer {

    public boolean write(Queue queue) {
        while (queue.peek() != null) {
            System.out.println(queue.poll());
        }
        return true;
    }

    @Override
    public String write(Set<Long> ids, List<Message> messages, long beginTimestamp, long endTimestamp) {
        return null;
    }
}
