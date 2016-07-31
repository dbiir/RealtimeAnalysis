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
        if (args.length != 4) {
            System.out.println("Usage: java -jar Loader propertiesFilePath topic partitionIdBegin partitionIdEnd");
            System.exit(1);
        }

        String props = args[0];
        String topic = args[1];
        int partitionB = Integer.parseInt(args[2]);
        int partitionE = Integer.parseInt(args[3]);
        List<Integer> partitionIds = new ArrayList<>();
        for (int i = partitionB; i <= partitionE; i++) {
            partitionIds.add(i);
        }

        ConfigFactory configFactory = ConfigFactory.getInstance(props);

        Loader loader = new Loader(topic, partitionIds);
        loader.load();
    }
}
