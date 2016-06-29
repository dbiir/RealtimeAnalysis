package cn.edu.ruc.realtime.utils;

import java.util.HashMap;

/**
 * Created by Jelly on 6/29/16.
 */
public class LogFactory {
    private HashMap<String, Log> logHashMap;
    private ConfigFactory config;
    private static LogFactory instance = null;

    private LogFactory() {
        logHashMap = new HashMap<>();
        config = ConfigFactory.getInstance();
        String[] logProps = config.getProps("logs").split(",");
        for (String prop: logProps) {
            logHashMap.put(prop.trim(), new Log(prop.trim()));
        }
    }

    public static LogFactory getInstance() {
        if (instance == null) {
            instance = new LogFactory();
        }
        return instance;
    }

    /**
     * Get logger by name
     * @param name logger name
     * */
    public Log getLogger(String name) {
        if (name == null)
            return null;
        return logHashMap.get(name);
    }
}

