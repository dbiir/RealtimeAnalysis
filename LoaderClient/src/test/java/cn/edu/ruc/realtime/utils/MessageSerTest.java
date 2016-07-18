package cn.edu.ruc.realtime.utils;

import cn.edu.ruc.realtime.model.Message;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

/**
 * RealTimeAnalysis
 *
 * @author Jelly
 */
public class MessageSerTest {

    public static void main(String[] args) {

        String s = "thisis a test中国";

        byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
        int len = bytes.length;
        ByteBuffer bbf = ByteBuffer.allocate(s.getBytes().length);
        for (int i = 0; i < len; i++) {
            bbf.put(i, bytes[i]);
        }
        bbf.put(bytes);

        String ss = new String(bbf.array(), StandardCharsets.UTF_8);

//        System.out.println(ss);

        Message msg1 = new Message(111, "this is a test中国");
        msg1.setTimestamp(1000000);
        Message msg2 = Message.fromBytes(msg1.toBytes());
        System.out.println(msg2.toString());
    }
}
