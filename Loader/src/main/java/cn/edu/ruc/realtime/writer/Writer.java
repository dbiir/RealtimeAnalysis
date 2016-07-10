package cn.edu.ruc.realtime.writer;

import cn.edu.ruc.realtime.model.Batch;

import java.util.Collection;
import java.util.Queue;

/**
 * Created by Jelly on 6/30/16.
 */
public interface Writer<T> {

    public boolean write(Queue<Batch<T>> queue);
}
