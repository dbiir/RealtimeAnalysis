package cn.ruc.edu.realtime.benchmark;

import cn.edu.ruc.realtime.client.LoaderClient;
import cn.edu.ruc.realtime.model.Message;
import cn.edu.ruc.realtime.utils.Function0;
import cn.ruc.edu.realtime.generator.Lineorder;
import cn.ruc.edu.realtime.generator.LineorderGenerator;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static net.sourceforge.argparse4j.impl.Arguments.store;

/**
 * RealTimeAnalysis
 *
 * @author guodong
 */
public class BenchmarkProducer
{
    private static final String REDIS_KEY = "lineorder10";
    private static final int PIPELINE_SIZE = 1000;
    // params: --topic TOPIC1 --scale-factor 10 --redis LOCALHOST:3456 --fiber-num 80 config
    public static void main(String[] args)
    {
        ArgumentParser parser = argParser();
        try
        {
            Namespace namespace = parser.parseArgs(args);
            String topicName = namespace.getString("topic");
            String redisHost = namespace.getString("redisHost");
            int redisPort = namespace.getInt("redisPort");
            String configFile = namespace.getString("configFile");
            int sf = namespace.getInt("sf");
            int fiberNum = namespace.getInt("fiberNum");

            Jedis jedis = new Jedis(redisHost, redisPort);
            Pipeline redisPipeline = jedis.pipelined();

            Iterator<Lineorder> iterator = new LineorderGenerator(sf, 10, 100).iterator();
//            final LoaderClient client = new LoaderClient(topicName, configFile);

            final Function0 function = new Function0(fiberNum);

            long pushStart = System.currentTimeMillis();
            // send generated lineorder data into redis
            long pushCount = 0L;
            while (iterator.hasNext())
            {
                Lineorder lineorder = iterator.next();
                redisPipeline.rpush(REDIS_KEY, lineorder.toLine());
                pushCount++;
                if (pushCount % PIPELINE_SIZE == 0)
                {
                    redisPipeline.sync();
                }
            }
            long pushEnd = System.currentTimeMillis();
            System.out.println("Push num: " + pushCount + ", push cost: " + (pushEnd - pushStart) + "ms");

            long pullCount = 0L;
            pushCount = pushCount - (pullCount % PIPELINE_SIZE);
            long pullStart = System.currentTimeMillis();
            List<Response> responses = new ArrayList<>(PIPELINE_SIZE);
            while (pullCount < pushCount)
            {
                responses.add(redisPipeline.lpop(REDIS_KEY));
                pullCount++;
                if (pullCount % PIPELINE_SIZE == 0)
                {
                    redisPipeline.sync();
                    responses.parallelStream().forEach(res -> {
                        String line = (String) res.get();
                        String[] lineParts = line.split("\\|");
                        Message message = new Message(function.apply(lineParts[0]), line);
                        message.setTimestamp(Long.parseLong(lineParts[24]));
                        System.out.println(message.toString());
//                        client.sendMessage(message);
                    });
                    responses.clear();
                }
            }
            long pullEnd = System.currentTimeMillis();
            System.out.println("Pull num: " + pullCount + ", pull cost: " + (pullEnd - pullStart) + "ms");
        }
        catch (ArgumentParserException e)
        {
            e.printStackTrace();
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

        parser.addArgument("--redis-host")
                .required(true)
                .action(store())
                .type(String.class)
                .metavar("REDIS-HOST")
                .dest("redisHost")
                .help("Host of redis server");

        parser.addArgument("--redis-port")
                .required(true)
                .action(store())
                .type(Integer.class)
                .metavar("REDIS-PORT")
                .dest("redisPort")
                .help("Port of redis server");

        parser.addArgument("--config-file")
                .required(true)
                .action(store())
                .type(String.class)
                .metavar("CONFIG-FILE")
                .dest("configFile")
                .help("Path of config file");

        return parser;
    }
}
