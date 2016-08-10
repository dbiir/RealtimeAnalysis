//package cn.ruc.edu.realtime.generator;
//
//import cn.edu.ruc.realtime.model.Message;
//import cn.ruc.edu.realtime.generator.RTDataGen;
//
//import java.util.Iterator;
//import java.util.LinkedList;
//import java.util.List;
//
///**
// * @author Jelly
// */
//public class RTDataGenTest {
//    private static String dataF = "/Users/Jelly/Developer/RealTimeAnalysis/RealTimeBenchmark/lineorderaa";
//    private static int threshold = 1000;
//
//    public static void main(String[] args) {
//        RTDataGen dataGen = new RTDataGen(dataF);
//
//        List<Message> msgList = new LinkedList<Message>();
//        msgList = dataGen.generator();
//        Iterator<Message> iterator = msgList.iterator();
//        int tCounter = 0;
//        while (iterator.hasNext()) {
//            if (tCounter >= threshold) {
//                break;
//            }
//            Message msg = iterator.next();
//            tCounter++;
//            System.out.println("Key: " + msg.getKey() + ", Value: " + msg.getValue());
//        }
//        System.out.println("End of RTDataGen test");
//    }
//}
