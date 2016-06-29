package cn.edu.ruc.realtime.utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

/**
 * Created by Jelly on 6/29/16.
 * Config Properties
 */
public class ConfigFactory {
    // props file path
    private static String propPath = "loader.props";
    private static Properties properties;
    private static ConfigFactory instance = null;

    private ConfigFactory(String path) {
        if (path != null || path != "")
            propPath = path;
        properties = new Properties();
        try {
            properties.load(new FileInputStream(new File(propPath)));
        } catch (IOException e) {
            // TODO terminate
            e.printStackTrace();
        }
    }

    private ConfigFactory() {
        try {
            properties.load(new FileInputStream(new File(propPath)));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * get config instance with specifying config file path
     * */
    public static ConfigFactory getInstance(String path) {
        if (instance == null) {
            instance = new ConfigFactory(path);
        }
        return instance;
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
