package cn.edu.ruc.realtime.utils;

import org.junit.Test;

/**
 * Created by Jelly on 6/29/16.
 */
public class ConfigFactoryTest {

    ConfigFactory config = ConfigFactory.getInstance();

    @Test
    public void test() {
        System.out.println(config.getBlockingQueueSize());
    }
}
