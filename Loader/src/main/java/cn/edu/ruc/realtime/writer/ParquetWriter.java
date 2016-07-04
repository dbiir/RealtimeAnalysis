package cn.edu.ruc.realtime.writer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.IntegerValue;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;

import java.io.IOException;
import java.util.Queue;

/**
 * Created by Jelly on 6/30/16.
 * Parquet Writer
 */
public class ParquetWriter implements Writer {
    private Configuration conf = new Configuration();
    private int blockSize = 256 * 1024 * 1024;
    private int pageSize = 1 * 1024 * 1024;
    private int dictionaryPageSize = 512;
    private boolean enableDictionary = false;
    private boolean validating = false;
    private CompressionCodecName compressionCodecName = CompressionCodecName.UNCOMPRESSED;
    private ParquetProperties.WriterVersion writerVersion = ParquetProperties.WriterVersion.PARQUET_2_0;
    private Path base = new Path("");
    private MessageType schema = MessageTypeParser.parseMessageType("message lineorders {" +
            "required int32 custkey; " +
            "required int32 orderkey; " +
            "required int32 partkey; " +
            "required int32 suppkey; " +
            "required int32 linenumber; " +
            "required float quantity; " +
            "required float extendedprice; " +
            "required float discount; " +
            "required float tax; " +
            "required fixed_len_byte_array(1) returnflag; " +
            "required fixed_len_byte_array(1) linestatus; " +
            "required binary shipdate; " +
            "required binary commitdate; " +
            "required binary receiptdate; " +
            "required fixed_len_byte_array(25) shipinstruct; " +
            "required fixed_len_byte_array(10) shipmode; " +
            "required binary comment; " +
            "required fixed_len_byte_array(1) orderstatus; " +
            "required float totalprice; " +
            "required binary orderdate; " +
            "required fixed_len_byte_array(15) orderpriority; " +
            "required fixed_len_byte_array(15) clerk; " +
            "required int32 shippriority; " +
            "required binary ordercomment; " +
            "}");
    private GroupWriteSupport groupWriteSupport = new GroupWriteSupport();
    private SimpleGroupFactory simpleGroupFactory = new SimpleGroupFactory(schema);
    private org.apache.parquet.hadoop.ParquetWriter parquetWriter;

    public ParquetWriter() {
        try {
            parquetWriter = new org.apache.parquet.hadoop.ParquetWriter(
                    base,
                    groupWriteSupport,
                    compressionCodecName,
                    blockSize, pageSize, dictionaryPageSize,
                    enableDictionary, validating,
                    writerVersion,
                    conf
            );
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void write(Queue queue) {
        groupWriteSupport.setSchema(schema, conf);
        while (queue.peek() != null) {
            String record = (String) queue.poll();
            System.out.println(record);
            String[] recordS = record.split("|");
            try {
                parquetWriter.write(
                        simpleGroupFactory.newGroup()
                        .append("custkey", Integer.parseInt(recordS[0]))
                        .append("orderkey", Integer.parseInt(recordS[1]))
                        .append("partkey", Integer.parseInt(recordS[2]))
                        .append("suppkey", Integer.parseInt(recordS[3]))
                        .append("linenumber", Integer.parseInt(recordS[4]))
                        .append("quantity", Float.parseFloat(recordS[5]))
                        .append("extendedprice", Float.parseFloat(recordS[6]))
                        .append("discount", Float.parseFloat(recordS[7]))
                        .append("tax", Float.parseFloat(recordS[8]))
                        .append("returnflag", recordS[9])
                        .append("linestatus", recordS[10])
                        .append("shipdate", recordS[11])
                        .append("commitdate", recordS[12])
                        .append("receiptdate", recordS[13])
                        .append("shipinstruct", recordS[14])
                        .append("shipmode", recordS[15])
                        .append("comment", recordS[16])
                        .append("orderstatus", recordS[17])
                        .append("totalprice", Float.parseFloat(recordS[18]))
                        .append("orderdate", recordS[19])
                        .append("orderprioriy", recordS[20])
                        .append("clerk", recordS[21])
                        .append("shippriority", recordS[22])
                        .append("ordercomment", recordS[23])
                );
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
