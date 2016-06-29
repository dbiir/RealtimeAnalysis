package cn.edu.ruc.realtime.client;

import cn.edu.ruc.realtime.model.Message;
import cn.edu.ruc.realtime.threads.LoaderClientPool;

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
    private LoaderClientPool loaderPool;
    private String topic;
    private String schema;

    /**
     * @param topic every client has one topic
     * */
    public LoaderClient(String topic, String schema) {
        this.topic = topic;
        this.schema = schema;
        loaderPool = new LoaderClientPool(topic);
        loaderPool.execute();
        //createTopic(schema);
    }

    /**
     * Create a topic
     * @param schema schema of topic
     * */
    private void createTopic(String schema) {
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
        System.out.println("Client for " + topic + " shutdown");
    }
}
