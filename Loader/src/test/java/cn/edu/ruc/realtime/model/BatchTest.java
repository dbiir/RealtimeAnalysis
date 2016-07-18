package cn.edu.ruc.realtime.model;

import java.util.ArrayDeque;
import java.util.Queue;

/**
 * Created by Jelly on 7/10/16.
 */
public class BatchTest {
    static Queue<Batch> queue = new ArrayDeque<>(50);

    public static void main(String[] args) {


        System.out.println(queue.size());
        System.out.println(queue.poll().toString());
    }
}
