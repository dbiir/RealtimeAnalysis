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
    private static String propPath = "./loader.props";
    private static Properties properties;
    private static ConfigFactory instance = null;
    private static Log systemLogger;

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
        systemLogger = LogFactory.getInstance().getSystemLogger();
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
    private String getProps(String key) throws PropertyNotExistException {
        if (instance == null || properties.getProperty(key) == null) {
            throw new PropertyNotExistException("Property " + key + " does not exist in config file.");
        }
        return properties.getProperty(key).trim();
    }

    // TODO implement all properties get method, handler all exceptions in place
    public String getLogDir() {
        try {
            return getProps("log.dir");
        } catch (PropertyNotExistException e) {
            e.printStackTrace();
        }
        return "./loader.props";
    }

    public String[] getLogs() {
        try {
            return getProps("logs").split("\\s*,\\s*");
        } catch (PropertyNotExistException e) {
            systemLogger.exception(e);
        }
        return new String[]{"system.log", "user.log"};
    }

    public int getBlockingQueueSize() {
        try {
            return Integer.parseInt(getProps("blocking.queue.size"));
        } catch (PropertyNotExistException e) {
            systemLogger.exception(e);
        } catch (NumberFormatException ne) {
            systemLogger.exception(ne);
        }
        return 1024;
    }

    public int getWriterThreadNum() {
        try {
            return Integer.parseInt(getProps("writer.thread.num"));
        } catch (PropertyNotExistException pe) {
            systemLogger.exception(pe);
        } catch (NumberFormatException ne) {
            systemLogger.exception(ne);
        }
        return 1;
    }

    /**
     * Get kafka consumer bootstrap servers
     * If not specified, default to "127.0.0.1:9092"
     * */
    public String getBootstrapServers() {
        try {
            return getProps("bootstrap.servers");
        } catch (PropertyNotExistException e) {
            systemLogger.exception(e);
        }
        return "127.0.0.1:9092";
    }

    public String getConsumerGroupId() {
        try {
            return getProps("consumer.group.id");
        } catch (PropertyNotExistException e) {
            systemLogger.exception(e);
        }
        return "test";
    }

    public String getConsumerAutoCommit() {
        try {
            String isAuto = getProps("consumer.auto.commit");
            if (isAuto == "true" || isAuto == "TRUE") {
                return "true";
            }
            if (isAuto == "false" || isAuto == "FALSE") {
                return "false";
            }
        } catch (PropertyNotExistException e) {
            systemLogger.exception(e);
        }
        return "true";
    }

    public String getConsumerAutoCommitInterval() {
        try {
            return getProps("consumer.auto.commit.interval.ms");
        } catch (PropertyNotExistException e) {
            systemLogger.exception(e);
        }
        return "1000";
    }

    public String getConsumerSessionTimeout() {
        try {
            return getProps("consumer.session.timeout");
        } catch (PropertyNotExistException e) {
            systemLogger.exception(e);
        }
        return "30000";
    }

    public String getConsumerKeyDeserializer() {
        try {
            return getProps("consumer.key.deserializer");
        } catch (PropertyNotExistException e) {
            systemLogger.exception(e);
        }
        return "org.apache.kafka.common.serialization.StringDeserializer";
    }

    public String getConsumerValueDeserializer() {
        try {
            return getProps("consumer.value.deserializer");
        } catch (PropertyNotExistException e) {
            systemLogger.exception(e);
        }
        return "org.apache.kafka.common.serialization.StringDeserializer";
    }

    public String getWriterFilePath() {
        try {
            return getProps("writer.file.path");
        } catch (PropertyNotExistException e) {
            systemLogger.exception(e);
        }
        return "file:///result/";
    }

//    public long getWriterBlockSize() {
//        try {
//            return Long.parseLong(getProps("writer.block.size"));
//        } catch (PropertyNotExistException pe) {
//            systemLogger.exception(pe);
//        } catch (NumberFormatException ne) {
//            systemLogger.exception(ne);
//        }
//        return 256L * 1024L;
//    }

    public float getWriterFullFactor() {
        try {
            return Float.parseFloat(getProps("writer.full.factor"));
        } catch (PropertyNotExistException pe) {
            systemLogger.exception(pe);
        } catch (NumberFormatException ne) {
            systemLogger.exception(ne);
        }
        return 0.98f;
    }

    /**
     * Batch consists of messages.
     * Get number of messages each batch.
     * */
    public int getWriterBatchSize() {
        try {
            return Integer.parseInt(getProps("writer.batch.size"));
        } catch (PropertyNotExistException pe) {
            systemLogger.exception(pe);
        } catch (NumberFormatException ne) {
            systemLogger.exception(ne);
        }
        return 1000;
    }

    /**
     * Block consists of batches.
     * Get number of batches each block.
     * */
    public int getWriterBlockSize() {
        try {
            return Integer.parseInt(getProps("writer.block.size"));
        } catch (PropertyNotExistException pe) {
            systemLogger.exception(pe);
        } catch (NumberFormatException ne) {
            systemLogger.exception(ne);
        }
        return 10;
    }

    public String getDBConnectionURL() {
        try {
            return getProps("db.connection.url");
        } catch (PropertyNotExistException pe) {
            systemLogger.exception(pe);
        }
        return "jdbc:postgresql://localhost/db";
    }

    public String getDBConnectionUser() {
        try {
            return getProps("db.connection.user");
        } catch (PropertyNotExistException pe) {
            systemLogger.exception(pe);
        }
        return "postgres";
    }

    public String getDBConnectionPwd() {
        try {
            return getProps("db.connection.pass");
        } catch (PropertyNotExistException pe) {
            systemLogger.exception(pe);
        }
        return "postgres";
    }

    public String getDBConnectionDriverClass() {
        try {
            return getProps("db.connection.driver.class");
        } catch (PropertyNotExistException pe) {
            systemLogger.exception(pe);
        }
        return "org.postgresql.Driver";
    }
}
