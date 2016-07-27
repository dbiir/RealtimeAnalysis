package cn.edu.ruc.realtime;

import cn.edu.ruc.realtime.model.Message;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Test;

import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by Jelly on 6/28/16.
 */
public class KafkaProducerTest {
    private int threadPoolSize = Runtime.getRuntime().availableProcessors() * 2;
    private static ExecutorService executor;

    public KafkaProducerTest() {
        executor = Executors.newFixedThreadPool(threadPoolSize);
    }

    public void execute() {
        for (int i = 0; i < 1; i++) {
            executor.execute(new KafkaProducerThread());
        }
        executor.shutdown();
    }

    public static void main(String[] args) {
        KafkaProducerTest kafkaProducerTest = new KafkaProducerTest();
        kafkaProducerTest.execute();
    }
}

class KafkaProducerThread implements Runnable {
    private Properties props = new Properties();
    private static Producer<Long, Message> producer;

    public KafkaProducerThread() {
        props.put("acks", "1");
        props.put("retries", 1);
        props.put("batch.size", 1024);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 55443322);
        props.put("bootstrap.servers", "127.0.0.1:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.LongSerializer");
        props.put("value.serializer", "cn.edu.ruc.realtime.utils.MessageSer");
//        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        // partition class
        props.put("partitioner.class", "cn.edu.ruc.realtime.partition.LoaderClientPartitionKafka");

        producer = new KafkaProducer(props);
    }

    int counter = 0;

    @Override
    public void run() {
        for (int i = 0; i < 6010; i++) {
            Message msg = new Message(i, "test000"+i);
            msg.setTimestamp(System.currentTimeMillis());
            producer.send(new ProducerRecord("07221918", 1L, msg));
            counter++;
        }
        producer.close();
        System.out.println("Done " + counter);
    }
}