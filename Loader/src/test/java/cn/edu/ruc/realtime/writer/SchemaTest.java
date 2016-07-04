package cn.edu.ruc.realtime.writer;

import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.junit.Test;

/**
 * Created by Jelly on 7/1/16.
 */
public class SchemaTest {
    private MessageType schema = MessageTypeParser.parseMessageType("message lineorders {" +
            "required int32 custkey; " +
            "required int32 orderkey; " +
            "required int32 partkey; " +
            "required int32 suppkey; " +
            "required int32 linenumber; " +
            "required float quantity; " +
            "required float extendedprice; " +
            "required float discount; " +
            "required float tax; " +
            "required fixed_len_byte_array(1) returnflag; " +
            "required fixed_len_byte_array(1) linestatus; " +
            "required binary shipdate; " +
            "required binary commitdate; " +
            "required binary receiptdate; " +
            "required fixed_len_byte_array(25) shipinstruct; " +
            "required fixed_len_byte_array(10) shipmode; " +
            "required binary comment; " +
            "required fixed_len_byte_array(1) orderstatus; " +
            "required float totalprice; " +
            "required binary orderdate; " +
            "required fixed_len_byte_array(15) orderpriority; " +
            "required fixed_len_byte_array(15) clerk; " +
            "required int32 shippriority; " +
            "required binary ordercomment; " +
            "}");

    @Test
    public void test() {
        SimpleGroupFactory simpleGroupFactory = new SimpleGroupFactory(schema);
        Group group = simpleGroupFactory.newGroup();
        for ( String[] s: schema.getPaths()) {
            System.out.println(s.length);
            for (String ss: s) {
                System.out.println(ss);
            }
        }
    }
}
