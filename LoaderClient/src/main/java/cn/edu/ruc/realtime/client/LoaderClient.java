package cn.edu.ruc.realtime.client;

import cn.edu.ruc.realtime.model.Message;
import cn.edu.ruc.realtime.threads.ProducerThread;
import cn.edu.ruc.realtime.threads.ProducerThreadPool;
import cn.edu.ruc.realtime.threads.KafkaProducerThread;
import cn.edu.ruc.realtime.threads.SimpleProducerThread;
import cn.edu.ruc.realtime.utils.ConfigFactory;
import cn.edu.ruc.realtime.utils.Log;
import cn.edu.ruc.realtime.utils.LogFactory;

import java.util.List;

/**
 * Created by Jelly on 6/12/16.
 * This is user-level programming interface.
 * Each LoaderClient corresponds to specific topic.
 *
 * Providing APIs:
 *      sendMessage(Message<S, T>);
 *      sendMessages(List<Message<S, T>>);
 *      shutdown();
 *
 * Example:
 * <code>
 *     String topic = "test";
 *     String schema = "message AddressBook { \
 *              required string owner; repeated string ownerPhoneNumbers; \
 *              repeated group contacts { \
 *                  required string name; \
 *                  optional string phoneNumber; \
 *              }
 *     }";
 *     LoaderClient client = new LoaderClient(topic, schema);
 *     Message<String, String> message = new Message("test1", "this is a test message");
 *     client.send(message);
 *     client.shutdown();
 * </code>
 */
public class LoaderClient {
    private ProducerThreadPool loaderPool;
    private String topic;
    private Log systemLogger;
    private final ConfigFactory configFactory;
    private final int producerTNum;

    /**
     * @param topic every client has one topic
     * */
    public LoaderClient(String topic, String config) {
        this.topic = topic;
        configFactory = ConfigFactory.getInstance(config);
        producerTNum = configFactory.getProducerThreadNum();
        loaderPool = new ProducerThreadPool(topic);
        for (int i = 0; i < producerTNum; i++) {
            ProducerThread thread = new KafkaProducerThread(topic, "KafkaProducer" + i, loaderPool.getQueue());
            loaderPool.addConsumer(thread);
        }
        loaderPool.execute();
        systemLogger = LogFactory.getInstance().getSystemLogger();
    }

    /**
     * Create table
     * @param table name of topic
     * @deprecated
     * */
    private void createTable(String table, int partitionNum, int replication) {
        // TODO create table
    }

    /**
     * Send a message
     * @param message content
     * */
    public void sendMessage(Message message) {
        loaderPool.put(message);
    }

    /**
     * Send messages
     * @param messages list of messages
     * */
    public void sendMessages(List<Message> messages) {
        for (Message m: messages) {
            sendMessage(m);
        }
    }

    /**
     * Shut down loader client
     * */
    public void shutdown() {
        loaderPool.shutdown();
        systemLogger.info("Client for " + topic + " hints to shutdown");
    }
}
