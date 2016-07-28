package cn.edu.ruc.realtime.loader;

import cn.edu.ruc.realtime.utils.ConfigFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * RealTimeAnalysis
 *
 * @author Jelly
 */
public class LoaderTest {

    public static void main(String[] args) {
        if (args.length < 3) {
            System.out.println("Usage: java -jar Loader propertiesFilePath topic partitionId partitionId ...");
            System.exit(1);
        }

        String props = args[0];
        String topic = args[1];
        List<Integer> partitionIds = new ArrayList<>();
        for (int i = 2; i < args.length; i++) {
            partitionIds.add(Integer.parseInt(args[i]));
        }

        ConfigFactory configFactory = ConfigFactory.getInstance(props);

        Loader loader = new Loader(topic, partitionIds);
        loader.load();
    }
}
