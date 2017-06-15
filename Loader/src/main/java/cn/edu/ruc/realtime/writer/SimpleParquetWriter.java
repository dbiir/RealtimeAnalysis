package cn.edu.ruc.realtime.writer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * RealTimeAnalysis
 *
 * @author guodong
 */
public class SimpleParquetWriter extends ParquetWriter<Group>
{
    /**
     * Create a new ParquetWriter.
     *
     * @param file                 the file to create
     * @param writeSupport         the implementation to write a record to a RecordConsumer
     * @param compressionCodecName the compression codec to use
     * @param blockSize            the block size threshold
     * @param pageSize             the page size threshold
     * @param dictionaryPageSize   the page size threshold for the dictionary pages
     * @param enableDictionary     to turn dictionary encoding on
     * @param validating           to turn on validation using the schema
     * @param writerVersion        version of parquetWriter from {@link ParquetProperties.WriterVersion}
     * @param conf                 Hadoop configuration to use while accessing the filesystem
     * @throws IOException
     */
    public SimpleParquetWriter(Path file,
                               WriteSupport<Group> writeSupport,
                               CompressionCodecName compressionCodecName,
                               int blockSize, int pageSize, int dictionaryPageSize,
                               boolean enableDictionary, boolean validating,
                               ParquetProperties.WriterVersion writerVersion,
                               Configuration conf) throws IOException
    {
        super(file, writeSupport, compressionCodecName, blockSize, pageSize, dictionaryPageSize, enableDictionary, validating, writerVersion, conf);
    }

    public static class Builder extends ParquetWriter.Builder<Group, Builder>
    {
        private MessageType type = null;
        private Map<String, String> extraMetaData = new HashMap<String, String>();

        private Builder(Path file)
        {
            super(file);
        }

        public Builder withType(MessageType type)
        {
            this.type = type;
            return this;
        }

        public Builder withExtraMetaData(Map<String, String> extraMetaData)
        {
            this.extraMetaData = extraMetaData;
            return this;
        }

        @Override
        public Builder self()
        {
            return this;
        }

        /**
         * @param conf
         * @return an appropriate WriteSupport for the object model.
         */
        protected WriteSupport<Group> getWriteSupport(Configuration conf)
        {
            return new GroupWriteSupport(type, extraMetaData);
        }
    }
}
