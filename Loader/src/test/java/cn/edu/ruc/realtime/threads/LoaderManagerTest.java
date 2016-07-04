package cn.edu.ruc.realtime.threads;

/**
 * Created by Jelly on 6/29/16.
 */
public class LoaderManagerTest {

    public static void main(String[] args) {
        LoaderManager manager = new LoaderManager("test07010348", 10);
        manager.execute();
    }
}
