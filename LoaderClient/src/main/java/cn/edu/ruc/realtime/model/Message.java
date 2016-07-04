package cn.edu.ruc.realtime.model;

/**
 * Created by Jelly on 6/12/16.
 * Message model.
 * A message consists of a key and a value. The key must be a String or Integer or Long
 */
public class Message<K, V> extends Model {
    private K key;
    private V value;

    public Message(K key, V value) {
        this.key = key;
        this.value = value;
    }

    public K getKey() {
        return key;
    }

    public String getStringKey() {
        return String.valueOf(key);
    }

    public V getValue() {
        return value;
    }

    public String toString() {
        return String.valueOf(key) + ": " + String.valueOf(value);
    }
}
