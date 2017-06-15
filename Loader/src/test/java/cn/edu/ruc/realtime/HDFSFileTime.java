package cn.edu.ruc.realtime;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.sql.Timestamp;

/**
 * RealTimeAnalysis
 *
 * @author guodong
 */
public class HDFSFileTime
{
    public static void main(String[] args) throws IOException
    {
        Configuration conf = new Configuration();
        conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());

        String fileUri = args[0];
        FileSystem fileFS = FileSystem.get(URI.create(fileUri) ,conf);
        FileStatus fileStatus = fileFS.getFileStatus(new Path(fileUri));

        System.out.println("File creation time： "+new Timestamp(fileStatus.getModificationTime()).toString());
    }
}
