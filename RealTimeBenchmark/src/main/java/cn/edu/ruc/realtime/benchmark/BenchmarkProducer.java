package cn.edu.ruc.realtime.benchmark;

import cn.edu.ruc.realtime.client.LoaderClient;
import cn.edu.ruc.realtime.generator.Lineorder;
import cn.edu.ruc.realtime.model.Message;
import cn.edu.ruc.realtime.utils.Function0;
import cn.edu.ruc.realtime.generator.LineorderGenerator;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import static net.sourceforge.argparse4j.impl.Arguments.store;

/**
 * RealTimeAnalysis
 *
 * @author guodong
 */
public class BenchmarkProducer
{
    public static void main(String[] args)
    {
        ArgumentParser parser = argParser();
        try
        {
            Namespace namespace = parser.parseArgs(args);
            String topicName = namespace.getString("topic");
            String filePath = namespace.getString("filePath");
            String configPath = namespace.getString("configPath");
            int sf = namespace.getInt("sf");
            int fiberNum = namespace.getInt("fiberNum");
            String mode = namespace.getString("mode");

            BenchmarkProducer benchmarkProducer = new BenchmarkProducer();

            switch (mode.toUpperCase())
            {
                case "W": generateAndSpill(filePath, sf, true); break;
                case "R": readAndSend(filePath, topicName, configPath, fiberNum); break;
                case "WR": generateAndSend(filePath, sf, topicName, configPath, fiberNum); break;
                case "D": benchmarkProducer.generateAndSendDirect(sf, topicName, configPath, fiberNum); break;
                default: throw new ArgumentParserException("No matching mode!", parser);
            }
        }
        catch (ArgumentParserException e)
        {
            if (args.length == 0)
            {
                parser.printHelp();
                System.exit(0);
            }
            else
            {
                parser.handleError(e);
                System.exit(1);
            }
        } catch (IOException e)
        {
            e.printStackTrace();
        }
    }

    /**
     *
     * @param path file path for generated data
     * @param sf scale factor, 100 around 1GB data
     * @param flush flush or not when writing to disk, if not, reader can make use of page cache
     * @throws IOException
     */
    private static void generateAndSpill(String path, int sf, boolean flush) throws IOException
    {
        BufferedWriter writer = new BufferedWriter(new FileWriter(path));
        Iterator<Lineorder> iterator = new LineorderGenerator(sf, 10, 100).iterator();

        long writeStart = System.currentTimeMillis();
        long writeCount = 0L;
        while (iterator.hasNext())
        {
            Lineorder lineorder = iterator.next();
            writer.write(lineorder.toLine());
            writer.newLine();
            writeCount++;
        }
        if (flush)
        {
            writer.flush();
        }
        long writeEnd = System.currentTimeMillis();
        writer.close();
        System.out.println("Generated num of messages: " + writeCount + ", time cost: " + (writeEnd - writeStart) + "ms");
    }

    /**
     *
     * @param path file path for generated data
     * @param topicName kafka topic name
     * @param configPath path of configuration file for LoaderClient
     * @param fiberNum num of fibers
     * @throws IOException
     */
    private static void readAndSend(String path, String topicName, String configPath, int fiberNum) throws IOException
    {
        BufferedReader reader = new BufferedReader(new FileReader(path));
        final LoaderClient client = new LoaderClient(topicName, configPath);
        final Function0 function = new Function0(fiberNum);

        String line = null;
        long readStart = System.currentTimeMillis();
        long readCount = 0;
        while ((line = reader.readLine()) != null)
        {
            String[] lineParts = line.split("\\|");
            Message message = new Message(function.apply(lineParts[0]), line);
            message.setTimestamp(Long.parseLong(lineParts[24]));
            client.sendMessage(message);
            readCount++;
        }
        long readEnd = System.currentTimeMillis();
        reader.close();
        System.out.println("Sent num of messages: " + readCount + ", time cost: " + (readEnd - readStart) + "ms");
    }

    /**
     *
     * @param path file path for generated data
     * @param sf scale factor, 100 around 1GB data
     * @param topicName kafka topic name
     * @param configPath path of configuration file for LoaderClient
     * @param fiberNum num of fibers
     * @throws IOException
     */
    private static void generateAndSend(String path, int sf, String topicName, String configPath, int fiberNum) throws IOException
    {
        generateAndSpill(path, sf, false);
        readAndSend(path, topicName, configPath, fiberNum);
    }

    /**
     *
     * @param sf scale factor, 100 around 1GB data
     * @param topicName kafka topic name
     * @param configPath path of configuration file for LoaderClient
     * @param fiberNum num of fibers
     */
    private void generateAndSendDirect(int sf, String topicName, String configPath, int fiberNum)
    {
        final BlockingQueue<Message> blockingQueue = new ArrayBlockingQueue<>(10000);
        int scale = sf / 10;
        for (int i = 0; i < 10; i++)
        {
            Generator generator = new Generator(scale, fiberNum, blockingQueue);
            generator.run();
        }
        for (int i = 0; i < 1; i++)
        {
            Producer producer = new Producer(topicName, configPath, blockingQueue);
            producer.run();
        }
    }

    private static ArgumentParser argParser()
    {
        ArgumentParser parser = ArgumentParsers
                .newArgumentParser("BenchmarkProducer")
                .defaultHelp(true)
                .description("This is a benchmark tool for producer");

        parser.addArgument("--topic")
                .required(true)
                .action(store())
                .type(String.class)
                .metavar("TOPIC")
                .dest("topic")
                .help("Name of kafka topic");

        parser.addArgument("--scale-factor")
                .required(true)
                .action(store())
                .type(Integer.class)
                .metavar("SCALE-FACTOR")
                .dest("sf")
                .help("Scale factor of lineorder table, 100 is round 1GB size");

        parser.addArgument("--fiber-num")
                .required(true)
                .action(store())
                .type(Integer.class)
                .metavar("FIBER-NUM")
                .dest("fiberNum")
                .help("Num of fibers in table");

        parser.addArgument("--file-path")
                .required(true)
                .action(store())
                .type(String.class)
                .metavar("FILE-PATH")
                .dest("filePath")
                .help("Path of disk spill");

        parser.addArgument("--mode")
                .required(true)
                .action(store())
                .type(String.class)
                .choices("W", "R", "WR", "D")
                .metavar("MODE")
                .dest("mode")
                .help("Mode: W(Generate data and write to disk), R(Read from disk and send to Kafka), WR(W and R), D(Generate data and send to Kafka directly without spilling to disk)");

        parser.addArgument("--config-path")
                .required(true)
                .action(store())
                .type(String.class)
                .metavar("CONFIG-FILE")
                .dest("configPath")
                .help("Path of config file");

        return parser;
    }

    private class Generator implements Runnable
    {
        private final int scaleFactor;
        private final BlockingQueue<Message> blockingQueue;
        private final Function0 function;

        Generator(int scaleFactor, int fiberNum, BlockingQueue<Message> blockingQueue)
        {
            this.scaleFactor = scaleFactor;
            this.blockingQueue = blockingQueue;
            function = new Function0(fiberNum);
        }

        /**
         * When an object implementing interface <code>Runnable</code> is used
         * to create a thread, starting the thread causes the object's
         * <code>run</code> method to be called in that separately executing
         * thread.
         * <p>
         * The general contract of the method <code>run</code> is that it may
         * take any action whatsoever.
         *
         * @see Thread#run()
         */
        @Override
        public void run()
        {
            long start = System.currentTimeMillis();
            long count = 0L;
            final Iterator<Lineorder> iterator = new LineorderGenerator(scaleFactor, 10, 100).iterator();
            while (iterator.hasNext())
            {
                Lineorder lineorder = iterator.next();
                Message message = new Message(function.apply(lineorder.getCustomerKey()), lineorder.toLine());
                message.setTimestamp(lineorder.getMessageDate());
                try
                {
                    blockingQueue.put(message);
                    count++;
                } catch (InterruptedException e)
                {
                    e.printStackTrace();
                }
            }
            long end = System.currentTimeMillis();
            System.out.println("Generator num: " + count + ", cost: " + (end - start) + "ms");
        }
    }

    private class Producer implements Runnable
    {
        private final String topicName;
        private final String configPath;
        private final BlockingQueue<Message> blockingQueue;

        public Producer(String topicName, String configPath, BlockingQueue<Message> blockingQueue)
        {
            this.topicName = topicName;
            this.configPath = configPath;
            this.blockingQueue = blockingQueue;
        }

        /**
         * When an object implementing interface <code>Runnable</code> is used
         * to create a thread, starting the thread causes the object's
         * <code>run</code> method to be called in that separately executing
         * thread.
         * <p>
         * The general contract of the method <code>run</code> is that it may
         * take any action whatsoever.
         *
         * @see Thread#run()
         */
        @Override
        public void run()
        {
            final LoaderClient client = new LoaderClient(topicName, configPath);
            long count = 0L;
            Message message = null;
            long start = System.currentTimeMillis();
            try
            {
                while ((message = blockingQueue.poll(100, TimeUnit.SECONDS)) != null)
                {
                    client.sendMessage(message);
                    count++;
                }
                long end = System.currentTimeMillis();
                System.out.println("Sent num: " + count + ", cost: " + (end - start) + "ms");
            } catch (InterruptedException e)
            {
                e.printStackTrace();
            }
        }
    }
}
