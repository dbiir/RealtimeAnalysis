package cn.ruc.edu.realtime.generator;

import cn.edu.ruc.realtime.model.Message;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

/**
 * @author Jelly
 * Example data generator. Read from file.
 */
public class RTDataGen {
    private String dataF = "";
    private BufferedReader reader;

    public RTDataGen(String dataFile) {
        this.dataF = dataFile;
    }

    public List<Message> generator() {
        String line = "";
        List<Message> messageList = new LinkedList<Message>();
        try {
            reader = new BufferedReader(new FileReader(dataF));
            while ((line = reader.readLine()) != null) {
                long key = Long.parseLong(line.split("\\|")[0]);
                Message msg = new Message(key, line);
                messageList.add(msg);
            }
            reader.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return messageList;
    }
}
