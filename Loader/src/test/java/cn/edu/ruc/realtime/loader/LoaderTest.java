package cn.edu.ruc.realtime.loader;

import cn.edu.ruc.realtime.utils.ConfigFactory;

/**
 * RealTimeAnalysis
 *
 * @author Jelly
 */
public class LoaderTest {

    public static void main(String[] args) {
        ConfigFactory configFactory = ConfigFactory.getInstance(args[0]);

        Loader loader = new Loader("test01", 1);
        loader.load();
    }
}
