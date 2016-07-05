package cn.edu.ruc.realtime.threads;

import cn.edu.ruc.realtime.utils.ConfigFactory;
import cn.edu.ruc.realtime.utils.Log;
import cn.edu.ruc.realtime.utils.LogFactory;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by Jelly on 6/29/16.
 * Loader Thread. Pulling data from Kafka.
 */
public class LoaderThread implements Runnable {

    private String topic;
    private int partition;
    private BlockingQueue queue;
    private Log systemLogger = LogFactory.getInstance().getSystemLogger();
    private final ConfigFactory configFactory = ConfigFactory.getInstance();
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final AtomicBoolean paused = new AtomicBoolean(false);
    private final KafkaConsumer consumer;

    public LoaderThread(String topic, int partition, BlockingQueue queue) {
        this.topic = topic;
        this.partition = partition;
        this.queue = queue;

        Properties props = new Properties();
        props.put("bootstrap.servers", configFactory.getBootstrapServers());
        props.put("group.id", configFactory.getConsumerGroupId());
        props.put("enable.auto.commit", configFactory.getConsumerAutoCommit());
        props.put("auto.commit.interval.ms", configFactory.getConsumerAutoCommitInterval());
        props.put("session.timeout.ms", configFactory.getConsumerSessionTimeout());
        props.put("key.deserializer", configFactory.getConsumerKeyDeserializer());
        props.put("value.deserializer", configFactory.getConsumerValueDeserializer());

        consumer = new KafkaConsumer(props);
        TopicPartition topicPartition = new TopicPartition(topic, partition);
        assginPartition(topicPartition);
        // TODO seek to the beginning, just for test, should consult the meta server.
        consumer.seekToBeginning(topicPartition);
    }

    @Override
    public void run() {
        try {
            while (!closed.get()) {
                ConsumerRecords<String, String> records = consumer.poll(Long.MAX_VALUE);
                for (ConsumerRecord<String, String> record: records.records(new TopicPartition(this.topic, this.partition))) {
                    systemLogger.info(getName() + record.value());
                    try {
                        queue.put(record.value());
                    } catch (InterruptedException e) {
                        systemLogger.exception(e);
                    }
                }
            }
        } catch (WakeupException we) {
            if (!closed.get())
                throw we;
        } finally {
            consumer.close();
        }
    }

    public void assginPartition(TopicPartition topicPartition) {
        this.topic = topicPartition.topic();
        this.partition = topicPartition.partition();
        consumer.assign(Arrays.asList(topicPartition));
    }

    public String getTopic() {
        return this.topic;
    }

    public int getPartition() {
        return this.partition;
    }

    public String getName() {
        return "Loader-" + getTopic() + "-" + getPartition();
    }

    public void shutdown() {
        closed.set(true);
        consumer.wakeup();
//        this.shutdown();
        systemLogger.info(getName() + ": shutdown");
    }

    public void pause() {
        if (!paused.get()) {
            paused.set(true);
            consumer.pause();
            systemLogger.info(getName() + ": pause");
        }
    }

    public void wakeUp() {
        if (paused.get()) {
            paused.set(false);
            consumer.wakeup();
            systemLogger.info(getName() + ": wakeup");
        }
    }

    public String toString() {
        return this.getName();
    }
}
