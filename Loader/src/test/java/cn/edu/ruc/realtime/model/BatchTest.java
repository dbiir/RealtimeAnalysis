package cn.edu.ruc.realtime.model;

import java.util.ArrayDeque;
import java.util.Queue;

/**
 * Created by Jelly on 7/10/16.
 */
public class BatchTest {
    static Queue<Batch<Integer>> queue = new ArrayDeque<>(50);

    public static void main(String[] args) {
        int i = 0;
        while (i < 1000) {
            Batch<Integer> batch = new Batch<>(10, i);
            for (int j = 0; j < 11; j++) {
                if (batch.isFull()) {
                    queue.add(batch);
                    batch = null;
                } else {
                    batch.addMsg(j, j);
                }
            }
        }

        System.out.println(queue.size());
        System.out.println(queue.poll().toString());
    }
}
