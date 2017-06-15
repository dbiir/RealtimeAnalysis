package cn.edu.ruc.realtime.writer;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.GroupWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.MessageType;

import java.util.HashMap;
import java.util.Map;

import static org.apache.parquet.Preconditions.checkNotNull;
import static org.apache.parquet.schema.MessageTypeParser.parseMessageType;

/**
 * RealTimeAnalysis
 *
 * @author guodong
 */
public class GroupWriteSupport extends WriteSupport<Group>
{
    private static final String PARQUET_SCHEMA = "parquet.example.schema";

    public static void setSchema(MessageType type, Configuration conf)
    {
        conf.set(PARQUET_SCHEMA, type.toString());
    }

    public static MessageType getSchema(Configuration conf)
    {
        return parseMessageType(checkNotNull(conf.get(PARQUET_SCHEMA), PARQUET_SCHEMA));
    }

    private MessageType schema;
    private GroupWriter groupWriter;
    private Map<String, String> extraMetaData;

    public GroupWriteSupport()
    {
        this(null, new HashMap<String, String>());
    }

    GroupWriteSupport(MessageType schema)
    {
        this(schema, new HashMap<String, String>());
    }

    GroupWriteSupport(MessageType schema, Map<String, String> extraMetaData)
    {
        this.schema = schema;
        this.extraMetaData = extraMetaData;
    }

    /**
     * called first in the task
     *
     * @param configuration the job's configuration
     * @return the information needed to write the file
     */
    public WriteContext init(Configuration configuration)
    {
        if (schema == null) {
            schema = getSchema(configuration);
        }
        return new WriteContext(schema, this.extraMetaData);
    }

    /**
     * This will be called once per row group
     *
     * @param recordConsumer the recordConsumer to write to
     */
    public void prepareForWrite(RecordConsumer recordConsumer)
    {
        groupWriter = new GroupWriter(recordConsumer, schema);
    }

    /**
     * called once per record
     *
     * @param record one record to write to the previously provided record consumer
     */
    public void write(Group record)
    {
        groupWriter.write(record);
    }
}
