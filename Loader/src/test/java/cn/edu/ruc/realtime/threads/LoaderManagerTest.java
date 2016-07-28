package cn.edu.ruc.realtime.threads;

import cn.edu.ruc.realtime.utils.ConfigFactory;

import java.util.ArrayList;
import java.util.List;

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

        List<Integer> ids = new ArrayList<>();
        ids.add(10);

        ThreadManager manager = new ThreadManager("test07051224", ids);
        manager.execute();

        System.out.println("Loading...");
    }
}
