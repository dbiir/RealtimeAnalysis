package cn.edu.ruc.realtime.threads;

import cn.edu.ruc.realtime.utils.ConfigFactory;

/**
 * Created by Jelly on 6/29/16.
 */
public class LoaderManagerTest {

    public static void main(String[] args) {
        if (args.length != 1) {
            System.out.println("Usage: java -jar loaderClient propertiesFilePath");
            System.exit(1);
        }

        System.out.println(args[0]);

        // load config
        ConfigFactory configFactory = ConfigFactory.getInstance(args[0]);

        LoaderManager manager = new LoaderManager("test07051224", 10);
        manager.execute();

        System.out.println("Loading...");
    }
}
