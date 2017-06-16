package cn.edu.ruc.realtime.client;

import cn.edu.ruc.realtime.model.Message;
import cn.edu.ruc.realtime.utils.ConfigFactory;
import cn.edu.ruc.realtime.utils.Function0;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * RealTimeAnalysis
 *
 * actual: 261 byte
 * with timestamp: 275 byte
 *
 * @author Jelly
 */
public class BenchMarkTest {

    public static void main(String[] args) {

        if (args.length != 6) {
            System.out.println("Usage: java -jar loaderClient propertiesFilePath input topic speed length factor");
            System.exit(1);
        }

        System.out.println(args[0]);
        String props = args[0];
        String input = args[1];
        String topic = args[2];
        String speed = args[3];
        String length = args[4];
        String factor = args[5];

//        ConfigFactory config = ConfigFactory.getInstance(props);
        LoaderClient client = new LoaderClient(topic, props);

        try {
            contralSpeed(client, input, Integer.parseInt(speed), Integer.parseInt(length), Integer.parseInt(factor));
            client.shutdown();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void contralSpeed(LoaderClient client, String fileName, double speed,int length, int factor)throws IOException {
        List<Message> nodelist = new LinkedList<Message>();
        Function0 function0 = new Function0(80);
        Map<Long, Long> statistics = new HashMap<>();

        BufferedReader br = new BufferedReader(
                new InputStreamReader(
                        new FileInputStream(fileName)));

        String line;
        while((line = br.readLine())!=null){
            int i;
            for(i=0;i<line.length();i++)  if(line.charAt(i)=='|')  break;
            String Key=line.substring(0,i);
            //long l=Long.parseLong(Key);
            long l = function0.apply(Key);
            if (!statistics.containsKey(l)) {
                statistics.put(l, 0L);
            }
            statistics.put(l, statistics.get(l) + 1L);
            nodelist.add(new Message(l,line));
        }

        int cnt=(int)(speed*1024*1024*1.0/length);
        double t=1000.0/cnt;
        int block=1;
        while(t<10.0){
            t*=10;
            block*=10;
        }
        int f=(int)(t+0.5);

        long start=0;
        int num=0,mark=0;
        long total=0;

        long Start=System.currentTimeMillis();
        while (factor-- > 0) {
            for (Message node : nodelist) {

                if (num == 0 && mark == 1) {
                    long js = System.currentTimeMillis();
                    long Stop = f - (js - start);

                    if (Stop > 0) {
                        try {
                            Thread.sleep(Stop);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                    mark = 0;
                }
                if (num == 0 && mark == 0) {
                    start = System.currentTimeMillis();
                    mark = 1;
                    num = block;
                }

                // add timestamp; sent message
                long test = System.currentTimeMillis();
                String withTimestamp = node.getValue() + "|" + test;
                Message msg = new Message(node.getKey(), withTimestamp);
                msg.setTimestamp(test);
                client.sendMessage(msg);

                num--;
                total++;

                if ((test - Start) % 10000 == 0) {
                    System.out.println(test - Start);
                    System.out.println("Speed " + total * 1.0 * length / 1024 / (test - Start));
                }
            }
        }
        long End=System.currentTimeMillis();
        System.out.println("Total speed " + total*length*1000/1024/1024/(End-Start) + "MB/S. Cost: " + (End-Start) + " ms.");
        br.close();
        System.out.println("Statistics:");
        for (long k : statistics.keySet()) {
            System.out.println("Fiber id [" + k + "], message num: " + statistics.get(k));
        }
    }
}