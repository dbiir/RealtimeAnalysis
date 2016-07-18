package cn.edu.ruc.realtime.writer;

import cn.edu.ruc.realtime.model.Message;

import java.util.List;
import java.util.Set;

/**
 * @author Jelly
 */
public interface Writer {

    /**
     * Write out.
     * @param messages messages
     * @return if success, return filename
     * */
    public String write(Set<Integer> ids, List<Message> messages, long beginTimestamp, long endTimestamp);
}
