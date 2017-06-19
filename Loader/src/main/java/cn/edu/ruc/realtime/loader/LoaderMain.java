package cn.edu.ruc.realtime.loader;

import cn.edu.ruc.realtime.utils.ConfigFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * RealTimeAnalysis
 *
 * @author guodong
 */
public class LoaderMain
{
    public static void main(String[] args) {
        if (args.length != 1) {
            System.out.println("Usage: java -jar Loader.jar config");
            System.exit(1);
        }

        String props = args[0];

        ConfigFactory configFactory = ConfigFactory.getInstance(props);
        String topic = configFactory.getLoaderTopic();
        int partitionB = configFactory.getLoaderPartitionBegin();
        int partitionE = configFactory.getLoaderPartitionEnd();

        List<Integer> partitionIds = new ArrayList<>();
        for (int i = partitionB; i <= partitionE; i++) {
            partitionIds.add(i);
        }

        Loader loader = new Loader(topic, partitionIds);
        loader.load();
    }
}
