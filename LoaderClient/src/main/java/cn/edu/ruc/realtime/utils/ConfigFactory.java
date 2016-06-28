package cn.edu.ruc.realtime.utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

/**
 * Created by Jelly on 6/12/16.
 * Config properties.
 *  log.dir: directory path of logs
 *
 */
public class ConfigFactory {
    // props file path
    private String propPath = "loaderClient.props";
    private static Properties properties;
    private static ConfigFactory instance = null;

    private ConfigFactory() {
        properties = new Properties();
        try {
            properties.load(new FileInputStream(new File(propPath)));
        } catch (IOException e) {
            // TODO terminate
            e.printStackTrace();
        }
    }

    /**
     * get config instance
     * */
    public static ConfigFactory getInstance() {
        if (instance == null) {
            instance = new ConfigFactory();
        }
        return instance;
    }

    /**
     * get property by key
     * @param key prop name
     * */
    public String getProps(String key) {
        if (instance == null) {
            return null;
        }
        return properties.getProperty(key);
    }

    // TODO implement all properties get method, handler all exceptions in place
}
