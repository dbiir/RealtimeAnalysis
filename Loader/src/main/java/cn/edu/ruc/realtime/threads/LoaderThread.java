package cn.edu.ruc.realtime.threads;

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
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final AtomicBoolean paused = new AtomicBoolean(false);
    private final KafkaConsumer consumer;

    public LoaderThread(String topic, int partition, BlockingQueue queue) {
        this.topic = topic;
        this.partition = partition;
        this.queue = queue;

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "test");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

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
                    System.out.println("Loader> " + record.value());
                    try {
                        queue.put(record.value());
                    } catch (InterruptedException e) {
                        e.printStackTrace();
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
        return "Consumer " + topic + "-" + partition;
    }

    public void shutdown() {
        closed.set(true);
        consumer.wakeup();
    }

    public void pause() {
        if (!paused.get()) {
            paused.set(true);
            consumer.pause();
        }
    }

    public void wakeUp() {
        if (paused.get()) {
            paused.set(false);
            consumer.wakeup();
        }
    }
}
