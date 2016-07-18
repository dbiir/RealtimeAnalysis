package cn.edu.ruc.realtime.threads;

/**
 * RealTimeAnalysis
 *
 * @author ${User}
 */
public abstract class LoaderThread implements Runnable {

    /**
     * Get thread name.
     * */
    public abstract String getName();

    /**
     * Set isReadyToStop signal.
     * */
    public abstract void setReadyToStop();

    /**
     * Get isReadyToStop signal.
     * */
    public abstract boolean readyToStop();
}
