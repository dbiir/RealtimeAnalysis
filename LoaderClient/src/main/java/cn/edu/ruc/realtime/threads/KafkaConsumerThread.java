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
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by Jelly on 6/12/16.
 * Thread puts message into Kafka
 */
public class KafkaConsumerThread<K, V> extends ConsumerThread {
    private String topic;
    private String threadName;
    private ConfigFactory config = ConfigFactory.getInstance();
    private Producer<K, V> producer;
    private Properties props;
    private BlockingQueue<Message> queue;
    private Log systemLogger = LogFactory.getInstance().getSystemLogger();
    private AtomicBoolean isReadyToStop = new AtomicBoolean(false);
    private AtomicLong msgCounter = new AtomicLong(0L);

    public KafkaConsumerThread(String topic, String threadName, BlockingQueue<Message> queue) {
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
        producer.send(new ProducerRecord(topic, String.valueOf(message.getKey()), message.getValue()));
    }

    @Override
    public void run() {
        long before = System.currentTimeMillis();
        while (!readyToStop()) {
            try {
                sendMessage(queue.take());
                msgCounter.getAndIncrement();
            } catch (InterruptedException e) {
                systemLogger.exception(e);
            }
        }
        long end = System.currentTimeMillis();
        System.out.println("Sending " + msgCounter.get() + " messages. Cost: " + (end -before) + " ms");
    }

    public String getThreadName() {
        return threadName;
    }

    @Override
    public void setReadyToStop() {
        isReadyToStop.set(true);
    }

    public boolean readyToStop() {
        return isReadyToStop.get();
    }
}
