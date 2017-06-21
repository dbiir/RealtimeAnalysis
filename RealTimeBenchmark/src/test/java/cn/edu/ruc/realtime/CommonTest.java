package cn.edu.ruc.realtime;

import org.junit.Test;

import java.util.HashMap;
import java.util.concurrent.ThreadLocalRandom;

/**
 * RealTimeAnalysis
 *
 * @author guodong
 */
public class CommonTest
{
    @Test
    public void randomUniformTest()
    {
        int i = 0;
        HashMap<Long, Integer> stats = new HashMap<>();
        while (i < 100000)
        {
            long v = ThreadLocalRandom.current().nextLong(1000000, 1000100);
            if (stats.containsKey(v))
            {
                stats.put(v, stats.get(v)+1);
            }
            else
            {
                stats.put(v, 1);
            }
            i++;
        }
        for (long v : stats.keySet())
        {
            System.out.println("Key: " + v + ", count: " + stats.get(v));
        }
    }
}
