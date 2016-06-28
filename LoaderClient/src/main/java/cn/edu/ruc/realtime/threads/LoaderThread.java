package cn.edu.ruc.realtime.threads;

import cn.edu.ruc.realtime.model.Message;
import cn.edu.ruc.realtime.utils.ConfigFactory;
import org.apache.kafka.clients.producer.KafkaProducer;

import java.util.Properties;
import java.util.concurrent.BlockingQueue;

/**
 * Created by Jelly on 6/12/16.
 */
public class LoaderThread<S, T> implements Runnable {
    private String topic;
    private String threadName;
    private ConfigFactory config = ConfigFactory.getInstance();
    private KafkaProducer<T, S> producer;
    private Properties props;
    private BlockingQueue<Message> queue;

    public LoaderThread(String topic, String threadName, BlockingQueue<Message> queue) {
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
        props.put("partitioner.class", "cn.edu.ruc.realtime.partition.LoaderPartitionKafka");
        producer = new KafkaProducer<T, S>(props);
    }

    public void sendMessage(Message message) {
//        producer.send(new ProducerRecord<T, S>(topic, 1, (T) message.getKey(), (S) message.getValue()));
        System.out.println(threadName + ">\t" + message.toString());
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
