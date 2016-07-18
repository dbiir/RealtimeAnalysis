package cn.edu.ruc.realtime.writer;

import cn.edu.ruc.realtime.model.Batch;
import cn.edu.ruc.realtime.model.Message;

import java.util.Collection;
import java.util.Queue;

/**
 * @author Jelly
 */
public interface Writer {

    /**
     * Write out.
     * @param queue batches
     * @return if success, return true
     * */
    public boolean write(Queue<Batch> queue);
}
