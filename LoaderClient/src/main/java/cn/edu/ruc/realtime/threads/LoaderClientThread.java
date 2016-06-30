package cn.edu.ruc.realtime.threads;

import cn.edu.ruc.realtime.model.Message;
import cn.edu.ruc.realtime.utils.ConfigFactory;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.concurrent.BlockingQueue;

/**
 * Created by Jelly on 6/12/16.
 */
public class LoaderClientThread<K, V> implements Runnable {
    private String topic;
    private String threadName;
    private ConfigFactory config = ConfigFactory.getInstance();
    private static Producer<String, String> producer;
    private Properties props;
    private BlockingQueue<Message> queue;

    public LoaderClientThread(String topic, String threadName, BlockingQueue<Message> queue) {
        this.topic = topic;
        this.threadName = threadName;
        this.queue = queue;

        props = new Properties();
        props.put("acks", config.getProps("acks"));
        props.put("retries", Integer.parseInt(config.getProps("retries")));
        props.put("batch.size", Integer.parseInt(config.getProps("batch.size")));
        props.put("linger.ms", Integer.parseInt(config.getProps("linger.ms")));
        props.put("buffer.memory", Long.parseLong(config.getProps("buffer.memory")));
        props.put("bootstrap.servers", config.getProps("bootstrap.servers"));
        props.put("key.serializer", config.getProps("key.serializer"));
        props.put("value.serializer", config.getProps("value.serializer"));
        // partition class
        props.put("partitioner.class", config.getProps("partitioner.class"));
        producer = new KafkaProducer(props);
    }

    public void sendMessage(Message<String, String> message) {
        System.out.println(getThreadName() + ">\t" + message.toString());
        producer.send(new ProducerRecord<String, String>(topic, message.getKey(), message.getValue()));
    }

    @Override
    public void run() {
        try {
            while (true) {
                sendMessage(queue.take());
            }
        } catch (InterruptedException ite) {
            ite.printStackTrace();
        }
    }

    public String getTopic() {
        return topic;
    }

    public String getThreadName() {
        return threadName;
    }
}