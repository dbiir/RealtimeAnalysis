package cn.edu.ruc.realtime.threads;

/**
 * Created by Jelly on 7/14/16.
 */
public abstract class ProducerThread implements Runnable {

    public abstract String getThreadName();

    /**
     * Set isReadyToStop signal.
     * */
    public abstract void setReadyToStop();

    /**
     * Get isReadyToStop signal.
     * */
    public abstract boolean readyToStop();
}
