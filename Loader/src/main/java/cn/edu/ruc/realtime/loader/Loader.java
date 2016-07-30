package cn.edu.ruc.realtime.loader;

import cn.edu.ruc.realtime.threads.ThreadManager;

import java.util.List;

/**
 * Created by Jelly on 6/29/16.
 */
public class Loader {
    private String topic;
    private List<Integer> partitionIds;

    public Loader(String topic, List<Integer> partitionIds) {
        this.topic = topic;
        this.partitionIds = partitionIds;
    }

    public void load() {
        ThreadManager manager = new ThreadManager(topic, partitionIds);
        manager.execute();
    }
}
