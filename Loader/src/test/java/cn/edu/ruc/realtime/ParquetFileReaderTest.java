package cn.edu.ruc.realtime;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.page.DataPage;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.column.page.PageReader;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.junit.Test;

import java.io.IOException;

import static org.apache.parquet.format.converter.ParquetMetadataConverter.NO_FILTER;

/**
 * RealTimeAnalysis
 *
 * @author guodong
 */
public class ParquetFileReaderTest
{
    @Test
    public void read(String fileName) throws IOException
    {
        Path path = new Path(fileName);
        Configuration conf = new Configuration();
        conf.set("fs.hdfs.impl", DistributedFileSystem.class.getName());

        ParquetMetadata metadata = ParquetFileReader.readFooter(conf, path, NO_FILTER);
        ParquetFileReader reader = new ParquetFileReader(conf, metadata.getFileMetaData(), path, metadata.getBlocks(), metadata.getFileMetaData().getSchema().getColumns());
        PageReadStore pageReadStore;
        PageReader pageReader;
        DataPage page;
        while ((pageReadStore = reader.readNextRowGroup()) != null) {
            for (ColumnDescriptor cd: metadata.getFileMetaData().getSchema().getColumns()) {
                pageReader = pageReadStore.getPageReader(cd);
                page = pageReader.readPage();
            }
        }
    }
}
