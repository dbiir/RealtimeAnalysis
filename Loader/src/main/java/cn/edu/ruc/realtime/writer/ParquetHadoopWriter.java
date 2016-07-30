package cn.edu.ruc.realtime.writer;

import cn.edu.ruc.realtime.model.Message;
import cn.edu.ruc.realtime.utils.ConfigFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * @author Jelly
 * Parquet Writer
 */
public class ParquetHadoopWriter implements Writer {
    private Configuration conf = new Configuration();
    private ConfigFactory configFactory = ConfigFactory.getInstance();
    private final String basePath = configFactory.getWriterFilePath();
//    private final String basePath = "file:///Users/Jelly/Developer/RealTimeAnalysis/parquet/";
    private int blockSize = 256 * 1024 * 1024;
    private int pageSize = 1 * 1024 * 1024;
    private int dictionaryPageSize = 512;
    private boolean enableDictionary = false;
    private boolean validating = false;
    private CompressionCodecName compressionCodecName = CompressionCodecName.UNCOMPRESSED;
    private ParquetProperties.WriterVersion writerVersion = ParquetProperties.WriterVersion.PARQUET_2_0;
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
            "required int64 messagedate; " +
            "}");
    private GroupWriteSupport groupWriteSupport = new GroupWriteSupport();
    private SimpleGroupFactory simpleGroupFactory = new SimpleGroupFactory(schema);

    public ParquetHadoopWriter() {
            conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
            conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
    }

    /**
     * Parquet writer
     * */
    public boolean write(List<Message> messages, String filename) {
        Path file = new Path(filename);
        try {
            groupWriteSupport.setSchema(schema, conf);
            ParquetWriter parquetWriter = new ParquetWriter(
                    file,
                    groupWriteSupport,
                    compressionCodecName,
                    this.blockSize, this.pageSize, dictionaryPageSize,
                    enableDictionary, validating,
                    writerVersion,
                    conf
            );
            for (Message msg : messages) {
//                System.out.println(msg.getValue());
                String[] recordS = msg.getValue().split("\\|");
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
                                .append("orderpriority", recordS[20])
                                .append("clerk", recordS[21])
                                .append("shippriority", Integer.parseInt(recordS[22]))
                                .append("ordercomment", recordS[23])
                                .append("messagedate", Long.parseLong(recordS[24]))
                );
            }
            parquetWriter.close();
        }catch (IOException e){
            e.printStackTrace();
            return false;
        }
        return true;
    }

    @Override
    public String write(Set<Integer> ids, List<Message> messages, long beginTimestamp, long endTimestamp) {
        // calculate path name
        StringBuilder sb = new StringBuilder();
        int counter = 0;
        Iterator<Integer> iterator = ids.iterator();
        sb.append(basePath);
        while (iterator.hasNext() && counter < 5) {
            sb.append(iterator.next());
        }
        sb.append(beginTimestamp);
        sb.append(endTimestamp);
        sb.append((Math.random() * endTimestamp + beginTimestamp) * Math.random());
        // write
        if (write(messages, sb.toString())) {
            return sb.toString();
        }
        return null;
    }
}
