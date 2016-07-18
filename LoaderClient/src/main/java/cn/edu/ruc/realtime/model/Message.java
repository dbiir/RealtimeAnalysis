package cn.edu.ruc.realtime.model;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * @author Jelly
 *
 * Message model.
 * A message consists of a key and a value. The key must be a String or Integer or Long
 */
public class Message extends Model {
    private long key;
    private long timestamp;
    private String value;

    public Message(long key, String value) {
        this.key = key;
        this.value = value;
    }

    public long getKey() {
        return key;
    }

    public String getStringKey() {
        return String.valueOf(key);
    }

    public String getValue() {
        return value;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public long getTimestamp() {
        return this.timestamp;
    }

    public byte[] toBytes() {
        int longSize = Long.BYTES;
        int base = longSize * 2;
        byte[] valueB = value.getBytes(StandardCharsets.UTF_8);
        int valueS = valueB.length;
        int size = base + valueS;
        ByteBuffer bbuf = ByteBuffer.allocate(size);
        bbuf.putLong(0, key);
        bbuf.putLong(longSize, timestamp);
        for (int i = 0; i < valueS; i++) {
            bbuf.put(base+i, valueB[i]);
        }

        return bbuf.array();
    }

    public static Message fromBytes(byte[] bytes) {
        int longSize = Long.BYTES;
        int base = longSize * 2;
        ByteBuffer wrapper = ByteBuffer.wrap(bytes);
        Message msg;
        int valueSize = bytes.length - base;
        long key = wrapper.getLong();
        long timestamp = wrapper.getLong(longSize);
        byte[] valueB = new byte[valueSize];
        for (int i = base; i < bytes.length; i++) {
            valueB[i-base] = wrapper.get(i);
        }
        msg = new Message(key, new String(valueB, StandardCharsets.UTF_8));
        msg.setTimestamp(timestamp);

        return msg;
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(key).append("-").append(timestamp).append(": ").append(value);
        return sb.toString();
    }
}
