package cn.edu.ruc.realtime.writer;

import cn.edu.ruc.realtime.model.Message;
import cn.edu.ruc.realtime.utils.ConfigFactory;
import cn.edu.ruc.realtime.utils.Output;

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Set;

/**
 * @author Jelly
 */
public class FileWriter implements Writer {
    private BufferedWriter writer;
    private ConfigFactory configFactory = ConfigFactory.getInstance();
    private final String basePath = configFactory.getWriterFilePath();

    public FileWriter() {
    }

    @Override
    public synchronized String write(Set<Integer> ids, List<Message> messages, long beginTime, long endTime) {
        StringBuilder sb = new StringBuilder();
        int counter = 0;
        Iterator<Integer> iterator = ids.iterator();
        sb.append(basePath);
        while (iterator.hasNext() && counter < 5) {
            sb.append(iterator.next());
        }
        sb.append(beginTime);
        sb.append(endTime);
        sb.append((Math.random()*endTime+beginTime)*Math.random());
        writer = Output.getBufferedWriter(sb.toString(), 8*1024);
        for (Message msg: messages) {
            try {
                writer.write(msg.getStringKey() + "-" +msg.getTimestamp() + ": " + msg.getValue() + "\n");
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        try {
            System.out.println("Flush file");
            writer.flush();
            return sb.toString();
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }
}
