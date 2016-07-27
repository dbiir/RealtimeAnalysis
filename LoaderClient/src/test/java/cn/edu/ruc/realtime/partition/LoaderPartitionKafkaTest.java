package cn.edu.ruc.realtime.partition;

/**
 * Created by Jelly on 6/28/16.
 */
public class LoaderPartitionKafkaTest {

    public static void main(String[] args) {
        for (long i = 1000L; i < 2000L; i++) {
            System.out.println(LoaderClientPartitionDefault.getPartition(i, 10));
        }
    }
}
