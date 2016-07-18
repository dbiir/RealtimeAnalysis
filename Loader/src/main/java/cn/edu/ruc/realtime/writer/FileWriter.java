package cn.edu.ruc.realtime.writer;

import cn.edu.ruc.realtime.model.Batch;
import cn.edu.ruc.realtime.model.Message;
import cn.edu.ruc.realtime.utils.Output;
import org.apache.jasper.tagplugins.jstl.core.Out;

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.Iterator;
import java.util.Queue;

/**
 * @author Jelly
 */
public class FileWriter implements Writer {
    private BufferedWriter writer;

    public FileWriter(String path) {
        try {
            writer = Output.getBufferedWriter(path, 1*1024);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public synchronized boolean write(Queue<Batch> queue) {
        Message msg;
        while (queue.peek() != null) {
            try {
                Iterator<Message> iterator = queue.poll().getIterator();
                while (iterator.hasNext()) {
                    msg = iterator.next();
                    writer.write(msg.getKey() + "-" + msg.getTimestamp() + ": " + msg.getValue() + "\n");
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        try {
            System.out.println("Flush and shutdown");
            writer.flush();
            return true;
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }
}
