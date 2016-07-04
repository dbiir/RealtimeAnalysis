package cn.edu.ruc.realtime.utils;

import java.util.HashMap;

/**
 * Created by Jelly on 6/12/16.
 * Store {{Log}} instance
 */
public class LogFactory {
    private HashMap<String, Log> logHashMap;
    private ConfigFactory config;
    private static LogFactory instance = null;

    private LogFactory() {
        logHashMap = new HashMap<>();
        config = ConfigFactory.getInstance();
        String[] logProps = config.getLogs();
        for (String prop: logProps) {
            logHashMap.put(prop.trim(), new Log(prop.trim()));
        }
        // default logger(must have) system.log
        logHashMap.put("system.log", new Log("system.log"));
    }

    public static LogFactory getInstance() {
        if (instance == null) {
            instance = new LogFactory();
        }
        return instance;
    }

    /**
     * Get customized logger by name
     * @param name logger name
     * */
    public Log getLogger(String name) {
        if (name == null)
            return null;
        return logHashMap.get(name);
    }

    /**
     * Get system logger
     * */
    public Log getSystemLogger() {
        return logHashMap.get("system.log");
    }
}
