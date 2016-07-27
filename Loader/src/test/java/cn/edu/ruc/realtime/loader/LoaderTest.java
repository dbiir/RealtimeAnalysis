package cn.edu.ruc.realtime.loader;

import cn.edu.ruc.realtime.utils.ConfigFactory;

/**
 * RealTimeAnalysis
 *
 * @author Jelly
 */
public class LoaderTest {

    public static void main(String[] args) {
        if (args.length != 3) {
            System.out.println("Usage: java -jar Loader propertiesFilePath topic partition");
            System.exit(1);
        }

        String props = args[0];
        String topic = args[1];
        int partitionNum = Integer.parseInt(args[2]);

        ConfigFactory configFactory = ConfigFactory.getInstance(props);

        Loader loader = new Loader(topic, partitionNum);
        loader.load();
    }
}
