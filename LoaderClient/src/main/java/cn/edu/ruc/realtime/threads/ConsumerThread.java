package cn.edu.ruc.realtime.threads;

/**
 * Created by Jelly on 7/14/16.
 */
public abstract class ConsumerThread implements Runnable {
    public abstract String getThreadName();
    public abstract void setReadyToStop();
    public abstract boolean readyToStop();

    public void interrupt() {
        Thread.interrupted();
    }
}
