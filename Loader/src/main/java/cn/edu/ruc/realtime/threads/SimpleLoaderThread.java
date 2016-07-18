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

    private final ConfigFactory configFactory = ConfigFactory.getInstance();
    private final KafkaConsumer consumer;
    private final int batchSize = configFactory.getWriterBatchSize();
    private Log systemLogger = LogFactory.getInstance().getSystemLogger();

    private AtomicBoolean isReadyToStop = new AtomicBoolean(false);

    public SimpleLoaderThread(String topic, int partition, BlockingQueue<Batch> queue) {
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
        while (!readyToStop()){
            Batch batch = new Batch(batchSize, partition);
            ConsumerRecords<Long, Message> records = consumer.poll(1000);
            for (ConsumerRecord<Long, Message> record: records) {
                systemLogger.info(getName() + record.value());
                try {
                    if (batch.isFull()) {
                        queue.put(batch);
                        batch = new Batch(batchSize, partition);
                    }
                    batch.addMsg(record.value(), record.offset());
                } catch (InterruptedException e) {
                    systemLogger.exception(e);
                }
            }
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
