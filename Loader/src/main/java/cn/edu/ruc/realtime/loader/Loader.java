package cn.edu.ruc.realtime.loader;

import cn.edu.ruc.realtime.threads.ThreadManager;

/**
 * Created by Jelly on 6/29/16.
 */
public class Loader {
    private String topic;
    private int partitionNum;

    public Loader(String topic, int partitionNum) {
        this.topic = topic;
        this.partitionNum = partitionNum;
    }

    public void load() {
        ThreadManager manager = new ThreadManager(topic, partitionNum);
        manager.execute();
    }
}
