package cn.edu.ruc.realtime.utils;

import jdk.nashorn.internal.runtime.regexp.joni.Config;

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
    private static String propPath = "loaderClient.props";
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
        properties = new Properties();
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
