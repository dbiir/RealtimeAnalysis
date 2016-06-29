package cn.edu.ruc.realtime.writer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;

import java.io.IOException;
import java.util.Collection;
import java.util.LinkedList;
import java.util.concurrent.BlockingQueue;

/**
 * Created by Jelly on 6/29/16.
 */
public class ParquetWriterThread {
    private BlockingQueue<ConsumerRecord> queue;
    private Collection<ConsumerRecord> list = new LinkedList();

    public ParquetWriterThread(BlockingQueue queue) {
        this.queue = queue;
    }

    public void run() {
        Configuration conf = new Configuration();
        int blockSize = 1 * 1024;
        int pageSize = 1 * 1024;
        int dictionaryPageSize = 512;
        boolean enableDictionary = false;
        boolean validating = false;
        Path basePath = new Path("file:///Users/Jelly/Developer/test");
        MessageType schema = MessageTypeParser.parseMessageType("message test {" +
                "required binary id; " +
                "required binary content; " +
                "required int64 int64_field; " +
                "}");
        GroupWriteSupport writeSupport = new GroupWriteSupport();
        writeSupport.setSchema(schema, conf);
        SimpleGroupFactory groupFactory = new SimpleGroupFactory(schema);

        try {
            ParquetWriter<Group> parquetWriter = new ParquetWriter(
                    basePath,
                    writeSupport,
                    CompressionCodecName.UNCOMPRESSED,
                    blockSize, pageSize, dictionaryPageSize,
                    enableDictionary,
                    validating,
                    ParquetProperties.WriterVersion.PARQUET_2_0,
                    conf);
            for (int i = 0; i < 50000; i++) {
                parquetWriter.write(groupFactory.newGroup()
                        .append("id", "10")
                        .append("content", "test" + i)
                        .append("int64_field", Long.valueOf(i)));
            }
            parquetWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
//
//        if (queue.remainingCapacity() < 10) {
//            queue.drainTo(list);
//            for (ConsumerRecord record: list) {
//            }
//        }
    }
}
