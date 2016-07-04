package cn.edu.ruc.realtime.threads;

import cn.edu.ruc.realtime.model.Message;
import cn.edu.ruc.realtime.utils.ConfigFactory;
import cn.edu.ruc.realtime.utils.Log;
import cn.edu.ruc.realtime.utils.LogFactory;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.concurrent.BlockingQueue;

/**
 * Created by Jelly on 6/12/16.
 * Thread puts message into Kafka
 */
public class LoaderClientThread<K, V> implements Runnable {
    private String topic;
    private String threadName;
    private ConfigFactory config = ConfigFactory.getInstance();
    private Producer<K, V> producer;
    private Properties props;
    private BlockingQueue<Message> queue;
    private Log systemLogger = LogFactory.getInstance().getSystemLogger();

    public LoaderClientThread(String topic, String threadName, BlockingQueue<Message> queue) {
        this.topic = topic;
        this.threadName = threadName;
        this.queue = queue;

        props = new Properties();
        props.put("acks", config.getAcks());
        props.put("retries", config.getRetries());
        props.put("batch.size", config.getBatchSize());
        props.put("linger.ms", config.getLingerMs());
        props.put("buffer.memory", config.getBufferMemory());
        props.put("bootstrap.servers", config.getBootstrapServers());
        props.put("key.serializer", config.getKeySerializer());
        props.put("value.serializer", config.getValueSerializer());
        // partitioner class
        props.put("partitioner.class", config.getPartitioner());

        producer = new KafkaProducer(props);
    }

    public void sendMessage(Message<K, V> message) {
        producer.send(new ProducerRecord(topic, message.getKey(), message.getValue()));
    }

    @Override
    public void run() {
        try {
            while (true) {
                sendMessage(queue.take());
            }
        } catch (InterruptedException ite) {
            systemLogger.exception(ite);
        }
    }

    public String getTopic() {
        return topic;
    }

    public String getThreadName() {
        return threadName;
    }
}
