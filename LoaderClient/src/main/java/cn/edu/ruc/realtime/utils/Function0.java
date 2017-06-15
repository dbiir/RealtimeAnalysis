package cn.edu.ruc.realtime.utils;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

import java.nio.charset.StandardCharsets;
import java.util.Objects;

/**
 * RealTimeAnalysis
 *
 * @author guodong
 */
public class Function0
{
    private final int seed;
    private final HashFunction hasher;
    private final int fiberNum;

    public Function0(int seed, int fiberNUm) {
        this(seed, Hashing.murmur3_128(seed), fiberNUm);
    }

    public Function0(int seed, HashFunction hasher, int fiberNum)
    {
        this.seed = seed;
        this.hasher = hasher;
        this.fiberNum = fiberNum;
    }

    public Function0(int fiberNum)
    {
        this(1318007700, fiberNum);
    }

    public int getSeed()
    {
        return seed;
    }

    public long apply(String v)
    {
        long k = hasher.hashString(v.subSequence(0, v.length()), StandardCharsets.UTF_8).asLong();
        return ((k % fiberNum) + fiberNum) % fiberNum;
    }

    public long apply(int v)
    {
        return hasher.hashInt(v).asLong();
    }

    public long apply(long v)
    {
        return hasher.hashLong(v).asLong();
    }

    public int hashCode()
    {
        return Objects.hash(hasher, seed);
    }

    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        Function0 other = (Function0) obj;
        return Objects.equals(hashCode(), other.hashCode());
    }

    public String toString()
    {
        return "function0: murmur3_128(" + seed + ")";
    }
}
