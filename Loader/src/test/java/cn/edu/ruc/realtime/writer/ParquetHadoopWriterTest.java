package cn.edu.ruc.realtime.writer;


import cn.edu.ruc.realtime.model.Message;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Created by Jelly on 6/29/16.
 */
public class ParquetHadoopWriterTest {
    private static BufferedReader reader;

    public static void main(String[] args) {
        try {
            reader = new BufferedReader(new FileReader("/Users/Jelly/Developer/RealTimeAnalysis/resources/lineorderaa"));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        ParquetHadoopWriter writer = new ParquetHadoopWriter();
        long beginTimestamp = 100000000L;
        long endTimestamp = 200000000L;
        Set<Integer> ids = new HashSet<>();
        for (int i = 0; i < 5; i++) {
            ids.add(i);
        }

        List<Message> messages = new ArrayList<>();
        String line;
        try {
            while ((line = reader.readLine()) != null) {
                Message msg = new Message(1, line);
                messages.add(msg);
            }
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        writer.write(ids, messages, beginTimestamp, endTimestamp);
    }
}
