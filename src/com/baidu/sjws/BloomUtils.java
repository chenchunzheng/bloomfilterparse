package com.baidu.sjws;

import org.apache.commons.lang.StringUtils;
import redis.clients.util.MurmurHash;

/**
 * Created by IntelliJ IDEA
 * User: huangqingwei
 * DATE: 16-2-29
 * Time: 14:21
 * To change this template use: File | Settings | File and Code Templates | Includes | File Header
 */
public class BloomUtils {

    private static final int SEED = 0x7f3a21ea;
    public static final double LOG_618 = Math.log(0.618d);
    public static final double LOG_2 = Math.log(2);
    public static final double LOG_2_SQUARE = LOG_2 * LOG_2;

    /**
     * Calculate optimal bit size: M
     * See http://en.wikipedia.org/wiki/Bloom_filter#Probability_of_false_positives for more details
     * @param n expectedElements
     * @param p falsePositiveProbability
     * @return optimal bit size
     */
    public static int calcOptimalM(int n, float p){
//        return (int) Math.ceil(n * (Math.log(p) / LOG_618));
        return (int) (-n * Math.log(p) / ( LOG_2_SQUARE ));
    }


    /**
     * Calculate optimal function numbers K
     * See http://en.wikipedia.org/wiki/Bloom_filter#Probability_of_false_positives for more details
     * @param bitSize bit size
     * @param n expectedElements
     * @return optimal function numbers
     */
    public static int calcOptimalK(int bitSize, int n){
//        return (int) Math.ceil(Math.log(2) * (bitSize / n));
        return Math.max(1, (int) Math.round(bitSize / n * LOG_2));
    }

    /**
     * get the setbit offsets by MurmurHash
     * @param value filter key
     * @return offsets
     */
    public static long[] murmurHashOffset(String value, int hCount, int bitSize) {
        long[] offsets = new long[hCount];
        if (StringUtils.isBlank(value)) {
            return offsets;
        }
        byte[] b = value.getBytes();
        int hash1 = MurmurHash.hash(b, SEED);
        int hash2 = MurmurHash.hash(b, hash1);
        for (int i = 1; i <= hCount; ++i){
            int nextHash = hash1 + i * hash2;
            if (nextHash < 0) {
                nextHash = ~nextHash;
            }
            offsets[i-1] = nextHash % bitSize;
        }
        return offsets;
    }

}
