package cn.edu.ruc.realtime.generator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * RealTimeAnalysis
 *
 * @author guodong
 */
public class CustomerParquetWriter
{
    private Configuration conf = new Configuration();
    private int blockSize = 256 * 1024 * 1024;
    private int pageSize = 1 * 1024 * 1024;
    private int dictionaryPageSize = 512;
    private boolean enableDictionary = false;
    private boolean validating = false;
    private CompressionCodecName compressionCodecName = CompressionCodecName.UNCOMPRESSED;
    private ParquetProperties.WriterVersion writerVersion = ParquetProperties.WriterVersion.PARQUET_2_0;
    private MessageType schema = MessageTypeParser.parseMessageType(
            "message customer {" +
                    "required int64 customerkey; " +
                    "required binary name; " +
                    "required binary address; " +
                    "required int64 nationkey; " +
                    "required binary phone; " +
                    "required double accountbalance; " +
                    "required binary marketsegment; " +
                    "required binary comment; " +
                    "}");
    private GroupWriteSupport groupWriteSupport = new GroupWriteSupport();
    private SimpleGroupFactory simpleGroupFactory = new SimpleGroupFactory(schema);

    public static void main(String[] args)
    {
        if (args.length != 3)
        {
            System.out.println("ARGS: writeBasePath customerFilePath blockRecordNum");
            System.exit(-1);
        }

        String basePath = args[0];
        String customerPath = args[1];
        int blockSize = Integer.parseInt(args[2]);
        System.out.println("Writing customers as parquet. BasePath: " + basePath + ", customerPath: " + customerPath + ", blockRecordNum: " + blockSize);

        CustomerParquetWriter parquetWriter = new CustomerParquetWriter();

        try (BufferedReader reader = new BufferedReader(new FileReader(customerPath)))
        {
            String line = null;
            int index = 0;
            int blockIndex = 0;
            List<String> lines = new ArrayList<>(blockSize);
            while ((line = reader.readLine()) != null)
            {
                lines.add(line);
                index++;
                if (index >= blockSize)
                {
                    parquetWriter.write(basePath, blockIndex, lines);
                    blockIndex++;
                    index = 0;
                    lines.clear();
                }
            }
            System.out.println("Done writing customer as parquet. Block num: " + blockSize);
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
    }

    private CustomerParquetWriter()
    {
        conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
    }

    private void write(String basePath, int blockIndex, List<String> customers)
    {
        if (!basePath.endsWith("/"))
        {
            basePath = basePath + "/";
        }
        Path path = new Path(basePath + "customer" + blockIndex);
        try
        {
            groupWriteSupport.setSchema(schema, conf);
            ParquetWriter parquetWriter = new ParquetWriter(
                    path,
                    groupWriteSupport,
                    compressionCodecName,
                    this.blockSize, this.pageSize, dictionaryPageSize,
                    enableDictionary, validating,
                    writerVersion,
                    conf
            );
            for (String cust : customers)
            {
                String[] parts = cust.split("\\|");
                parquetWriter.write(
                        simpleGroupFactory.newGroup()
                        .append("customerkey", Long.parseLong(parts[0]))
                        .append("name", parts[1])
                        .append("address", parts[2])
                        .append("nationkey", Long.parseLong(parts[3]))
                        .append("phone", parts[4])
                        .append("accountbalance", Double.parseDouble(parts[5]))
                        .append("marketsegment", parts[6])
                        .append("comment", parts[7]));
            }
            parquetWriter.close();
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
    }
}
