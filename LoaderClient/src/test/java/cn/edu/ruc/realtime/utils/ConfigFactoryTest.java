package cn.edu.ruc.realtime.utils;

import org.junit.Test;
import static org.junit.Assert.assertEquals;

/**
 * Created by Jelly on 6/12/16.
 */
public class ConfigFactoryTest {
    @Test
    public void test() {
        ConfigFactory config = ConfigFactory.getInstance();
//        System.out.println(config.getProps("log.dir"));
        assertEquals(config.getProps("log.dir"), "loaderClient.log");
    }

}
