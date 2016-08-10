package cn.edu.ruc.realtime.writer;

import cn.edu.ruc.realtime.model.Message;
import cn.edu.ruc.realtime.utils.ConfigFactory;
import cn.edu.ruc.realtime.utils.Log;
import cn.edu.ruc.realtime.utils.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * RealTimeAnalysis
 *
 * @author Jelly
 */
public class HadoopWriter implements Writer {

    private ConfigFactory configFactory = ConfigFactory.getInstance();
    private final String basePath = configFactory.getWriterFilePath();
    private Log systemLogger = LogFactory.getInstance().getSystemLogger();
    private Configuration conf = new Configuration();
    private FSDataOutputStream outputStream;

    public HadoopWriter() {
        conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
    }

    @Override
    public synchronized String write(Set<Integer> ids, List<Message> messages, long beginTime, long endTime) {
        StringBuilder sb = new StringBuilder();
        int counter = 0;
        Iterator<Integer> iterator = ids.iterator();
        sb.append(basePath);
        while (iterator.hasNext() && counter < 5) {
            sb.append(iterator.next());
        }
        sb.append(beginTime);
        sb.append(endTime);
        sb.append((Math.random() * endTime + beginTime) * Math.random());

        try {
            FileSystem fileSystem = FileSystem.get(new URI("hdfs://192.168.7.33:9000"), conf);
            outputStream = fileSystem.create(new Path(sb.toString()));
            for (Message msg : messages) {
                outputStream.writeBytes(msg.getValue() + "\n");
            }
            outputStream.flush();
            System.out.println("Flush file");
            systemLogger.info("Flush file " + sb.toString());
        } catch (IOException e) {
            e.printStackTrace();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
        return sb.toString();
    }
}
