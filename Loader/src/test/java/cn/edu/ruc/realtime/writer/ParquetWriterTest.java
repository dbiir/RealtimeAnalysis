package cn.edu.ruc.realtime.writer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;

/**
 * RealTimeAnalysis
 *
 * @author guodong
 */
public class ParquetWriterTest
{
    @Test
    public void test() throws IOException
    {
        Type name = new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.BINARY, "name");
        Type age = new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.INT32, "age");
        Type score = new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.DOUBLE, "score");
        Type student = new MessageType("student", Arrays.asList(name, age, score));
        MessageType schema = new MessageType("student", student);

        int blockSize = 256 * 1024 * 1024;
        int pageSize = 6 * 1024;
        int dictionaryPageSize = 512;
        boolean enableDictionary = false;
        boolean validating = false;

        GroupWriteSupport groupWriteSupport = new GroupWriteSupport();
        SimpleGroupFactory simpleGroupFactory = new SimpleGroupFactory(schema);

        Configuration conf = new Configuration();
        conf.set("fs.hdfs.impl", DistributedFileSystem.class.getName());
        Path path = new Path("hdfs://127.0.0.1:9000/student.parquet");
        groupWriteSupport.setSchema(schema, conf);
        ParquetWriter parquetWriter = new ParquetWriter(
                path,
                groupWriteSupport,
                CompressionCodecName.UNCOMPRESSED,
                blockSize,
                pageSize,
                dictionaryPageSize,
                enableDictionary,
                validating,
                ParquetProperties.WriterVersion.PARQUET_2_0,
                conf);

    }
}
