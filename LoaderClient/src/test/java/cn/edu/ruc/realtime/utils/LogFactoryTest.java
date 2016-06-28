package cn.edu.ruc.realtime.utils;

import org.junit.Test;

/**
 * Created by Jelly on 6/13/16.
 */
public class LogFactoryTest {
    @Test
    public void test() {
        LogFactory logFactory = LogFactory.getInstance();
        Log sysLogger = logFactory.getLogger("system.log");
        Log userLogger = logFactory.getLogger("user.log");

        if (sysLogger == null || userLogger == null) {
            System.out.println("Logger does not exist!");
            return;
        }

        sysLogger.info("Started");
        userLogger.info("Query executing...");
        sysLogger.info("End");
    }
}
