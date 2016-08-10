package cn.ruc.edu.realtime.benchmark;

import java.util.HashSet;
import java.util.Set;

/**
 * RealTimeAnalysis
 *
 * @author Jelly
 */
public class Test {

    public static void main(String[] args) {
        Set<String> setS = new HashSet<String>();

        for (int i = 0; i < 100; i++) {
            setS.add(String.valueOf(i));
        }

        String[] ss = setS.toArray(new String[]{});

        for (int i = 0; i < ss.length; i++) {
            System.out.println(ss[i]);
        }
        System.out.println("Len: " + ss.length);
    }
}
