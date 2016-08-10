package cn.edu.ruc.realtime.threads;

import cn.edu.ruc.realtime.model.Batch;
import cn.edu.ruc.realtime.model.Message;
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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by Jelly on 6/29/16.
 * Loader Thread.
 * Each thread pulls data from specified partition of Kafka, and essemble data into {@link Batch} in thread, and then
 * put Batch into shared buffer. When shared buffer is ready to write, {@link SimpleWriterThread} will write shared
 * buffer block out.
 */
public class SimpleLoaderThread extends LoaderThread {

    private String topic;
    private int partition;
    private BlockingQueue<Batch> queue;
    private Properties props = new Properties();

    private final ConfigFactory configFactory = ConfigFactory.getInstance();
    private final int batchSize = configFactory.getWriterBatchSize();
    private Log systemLogger = LogFactory.getInstance().getSystemLogger();

    private KafkaConsumer consumer;

    private AtomicBoolean isReadyToStop = new AtomicBoolean(false);
    private AtomicLong counter = new AtomicLong(0L);
    private AtomicInteger batchCounter = new AtomicInteger(0);
    private final ThreadLocal<Batch> localBatch = new ThreadLocal<Batch>() {
        @Override
        protected Batch initialValue() {
            return new Batch(batchSize, partition);
        }
    };

    public SimpleLoaderThread(String topic, int partition, BlockingQueue<Batch> queue) {
        this.topic = topic;
        this.partition = partition;
        this.queue = queue;

        props.put("bootstrap.servers", configFactory.getBootstrapServers());
        props.put("group.id", configFactory.getConsumerGroupId());
        props.put("enable.auto.commit", configFactory.getConsumerAutoCommit());
        props.put("auto.commit.interval.ms", configFactory.getConsumerAutoCommitInterval());
        props.put("session.timeout.ms", configFactory.getConsumerSessionTimeout());
        props.put("key.deserializer", configFactory.getConsumerKeyDeserializer());
        props.put("value.deserializer", configFactory.getConsumerValueDeserializer());

    }

    @Override
    public void run() {
        consumer = new KafkaConsumer(props);
        TopicPartition topicPartition = new TopicPartition(topic, partition);
        System.out.println("Parition: " + topicPartition.partition());
        consumer.assign(Arrays.asList(topicPartition));
        // TODO seek to the beginning, just for test, should consult the meta server.
        consumer.seekToBeginning(topicPartition);
//        int msgCounter = 0;
//        int batchCounter = 0;

        while (true){
            if (readyToStop()) {
                try {
                    if (!localBatch.get().isEmpty()) {
                        queue.put(localBatch.get());
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                break;
            }
            ConsumerRecords<Long, Message> records = consumer.poll(Long.MAX_VALUE);
            for (ConsumerRecord<Long, Message> record: records) {
//                counter.getAndIncrement();
//                msgCounter++;
                try {
                    if (localBatch.get().isFull()) {
                        queue.put(localBatch.get());
//                        System.out.println(queue.size());
                        localBatch.set(new Batch(batchSize, partition));
//                        batchCounter.getAndIncrement();
//                        batchCounter++;
                    }
                    localBatch.get().addMsg(record.value(), record.offset());
                } catch (Exception e) {
                    systemLogger.exception(e);
                }
            }
//            systemLogger.info("Partition: " + getPartition() + ", message counter: " + msgCounter);
//            systemLogger.info("Partition: " + getPartition() + ", batch counter: " + batchCounter);
        }
    }

    public String getTopic() {
        return this.topic;
    }

    public int getPartition() {
        return this.partition;
    }

    public String getName() {
        return "SimpleLoaderThread-" + getTopic() + "-" + getPartition();
    }

    @Override
    public void setReadyToStop() {
        isReadyToStop.set(true);
    }

    @Override
    public boolean readyToStop() {
        return isReadyToStop.get();
    }

    public String toString() {
        return this.getName();
    }
}
