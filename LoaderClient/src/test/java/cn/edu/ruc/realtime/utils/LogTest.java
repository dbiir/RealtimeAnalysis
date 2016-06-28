package cn.edu.ruc.realtime.utils;

import org.junit.Test;

/**
 * Created by Jelly on 6/13/16.
 */
public class LogTest {
    @Test
    public void test() {
        Log systemLogger = new Log("system.log");
        Log userLogger = new Log("user.log");
        systemLogger.error("Fatal error, no config file found");
        systemLogger.info("Query 1");
        userLogger.info("Query 2");
        userLogger.warn("Query no result");
        Error e = new Error("Fatal error, query failed");
        Exception excp = new Exception("No file found");
        userLogger.error(e);
        systemLogger.exception(excp);
    }
}
