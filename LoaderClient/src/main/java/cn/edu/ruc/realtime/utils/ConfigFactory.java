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
    // default props file path
    private static String propPath = "./config.props";
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
     * @throws PropertyNotExistException
     * */
    private String getProps(String key) throws PropertyNotExistException {
        if (instance == null || properties.getProperty(key) == null) {
            throw new PropertyNotExistException("Property " + key + " not exists.");
        }
        return properties.getProperty(key).trim();
    }

    /**
     * Get log dir
     * If not specified, default to "./"
     * */
    public String getLogDir() {
        try {
            return getProps("log.dir");
        } catch (PropertyNotExistException e) {
            systemLogger.exception(e);
        }
        return "./";
    }

    /**
     * Get logger
     * If not specified, default to {"system.log", "user.log"}
     * */
    public String[] getLogs() {
        try {
            return getProps("logs").split("\\s*,\\s*");
        } catch (PropertyNotExistException e) {
            systemLogger.exception(e);
        }
        return new String[]{"system.log", "user.log"};
    }

    /**
     * Get kafka producer acks
     * If not specified, default to "1"
     * */
    public String getAcks() {
        try {
            return getProps("acks");
        } catch (PropertyNotExistException e) {
            systemLogger.exception(e);
        }
        return "1";
    }

    /**
     * Get kafka producer retries
     * If not specified, default to 1
     * */
    public int getRetries() {
        try {
            return Integer.parseInt(getProps("retries"));
        } catch (PropertyNotExistException e) {
            systemLogger.exception(e);
        }
        return 1;
    }

    /**
     * Get kafka producer batch size
     * If not specified, default to 1024
     * */
    public int getBatchSize() {
        try {
            return Integer.parseInt(getProps("batch.size"));
        } catch (PropertyNotExistException e) {
            systemLogger.exception(e);
        }
        return 1024;
    }

    /**
     * Get kafka producer buffer memory
     * If not specified, default to 33554432
     * */
    public long getBufferMemory() {
        try {
            return Long.parseLong(getProps("buffer.memory"));
        } catch (PropertyNotExistException e) {
            systemLogger.exception(e);
        }
        return 33554432;
    }

    /**
     * Get kafka producer bootstrap servers
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

    /**
     * Get kafka producer linger ms
     * If not specified, default to 1
     * */
    public int getLingerMs() {
        try {
            return Integer.parseInt(getProps("linger.ms"));
        } catch (PropertyNotExistException e) {
            systemLogger.exception(e);
        }
        return 1;
    }

    /**
     * Get kafka producer key serializer
     * If not specified, default to "org.apache.kafka.common.serialization.StringSerializer"
     * */
    public String getKeySerializer() {
        try {
            return getProps("key.serializer");
        } catch (PropertyNotExistException e) {
            systemLogger.exception(e);
        }
        return "org.apache.kafka.common.serialization.StringSerializer";
    }

    /**
     * Get kafka producer value serializer
     * If not specified, default to "org.apache.kafka.common.serialization.StringSerializer"
     * */
    public String getValueSerializer() {
        try {
            return getProps("value.serializer");
        } catch (PropertyNotExistException e) {
            systemLogger.exception(e);
        }
        return "org.apache.kafka.common.serialization.StringSerializer";
    }

    /**
     * Get kafka producer partitioner
     * If not specified, default to "org.apache.kafka.clients.producer.internal.Partitioner"
     *
     * */
    public String getPartitioner() {
        try {
            return getProps("partitioner.class");
        } catch (PropertyNotExistException e) {
            systemLogger.exception(e);
        }
        return "org.apache.kafka.clients.producer.internal.Partitioner";
    }

    /**
     * Get thread pool size
     * If not specified, default to <code>Runtime.getRuntime().availableProcessors()*2</code>
     * */
    public int getThreadPoolSize() {
        try {
            return Integer.parseInt(getProps("thread.pool.size"));
        } catch (PropertyNotExistException e) {
            systemLogger.exception(e);
        }
        return Runtime.getRuntime().availableProcessors() * 2;
    }

    public int getThreadQueueSize() {
        try {
            return Integer.parseInt(getProps("thread.queue.size"));
        } catch (PropertyNotExistException e) {
            systemLogger.exception(e);
        }
        return 0;
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
        return "org.apache.kafka.common.serialization.LongDeserializer";
    }

    public String getConsumerValueDeserializer() {
        try {
            return getProps("consumer.value.deserializer");
        } catch (PropertyNotExistException e) {
            systemLogger.exception(e);
        }
        return "cn.edu.ruc.realtime.utils.MessageDer";
    }

    public String getWriterFilePath() {
        try {
            return getProps("writer.file.path");
        } catch (PropertyNotExistException e) {
            systemLogger.exception(e);
        }
        return "file:///result/";
    }

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

    public int getProducerThreadNum() {
        try {
            return Integer.parseInt(getProps("producer.thread.num"));
        } catch (PropertyNotExistException pe) {
            systemLogger.exception(pe);
        } catch (NumberFormatException ne) {
            systemLogger.exception(ne);
        }
        return 1;
    }
}
