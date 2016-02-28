package com.baidu.sjws;

import org.apache.commons.lang.StringUtils;
import redis.clients.util.MurmurHash;

/**
 * Created by IntelliJ IDEA
 * User: huangqingwei
 * DATE: 16-2-26
 * Time: 18:54
 * To change this template use: File | Settings | File and Code Templates | Includes | File Header
 */
public class BloomFilter {



    private int maxKey;
    private float errorRate;
    private int hashFunctionCount;
    private int bitSize;

    public BloomFilter(int maxKey, float errorRate) {
        this.maxKey = maxKey;
        this.errorRate = errorRate;
        this.bitSize = calcOptimalM(maxKey, errorRate);
        this.hashFunctionCount = calcOptimalK(bitSize, maxKey);
    }

    /**
     * Calculate M and K
     * See http://en.wikipedia.org/wiki/Bloom_filter#Probability_of_false_positives for more details
     * @param maxKey
     * @param errorRate
     * @return
     */
    public int calcOptimalM(int maxKey, float errorRate){
        return (int) Math.ceil(maxKey
                * (Math.log(errorRate) / Math.log(0.6185)));
    }

    /**
     * Calculate M and K
     * See http://en.wikipedia.org/wiki/Bloom_filter#Probability_of_false_positives for more details
     * @param bitSize
     * @param maxKey
     * @return
     */
    public int calcOptimalK(int bitSize, int maxKey){
        return (int) Math.ceil(Math.log(2) * (bitSize / maxKey));
    }

    /**
     * get the setbit offset by MurmurHash
     * @param value
     * @return
     */
    public static int[] murmurHashOffset(String value, int hashFunctionCount, int maxBitCount) {
        int[] offsets = new int[hashFunctionCount];
        if (StringUtils.isBlank(value)) {
            return offsets;
        }
        byte[] b = value.getBytes();
        int hash1 = MurmurHash.hash(b, 0);
        int hash2 = MurmurHash.hash(b, hash1);
        for (int i = 0; i < hashFunctionCount; ++i){
            offsets[i] = (int) (Math.abs((hash1 + i * hash2) % maxBitCount) );
        }
        return offsets;
    }

    /**
     * get the setbit offset by MurmurHash
     * @param value
     * @return
     */
    public long[] murmurHashOffset(String value) {
        long[] offsets = new long[this.hashFunctionCount];
        if (StringUtils.isBlank(value)) {
            return offsets;
        }
        byte[] b = value.getBytes();
        int hash1 = MurmurHash.hash(b, 0);
        int hash2 = MurmurHash.hash(b, hash1);
        for (int i = 0; i < hashFunctionCount; ++i){
            offsets[i] = (int) (Math.abs((hash1 + i * hash2) % this.maxKey) );
        }
        return offsets;
    }

    public int getMaxKey() {
        return maxKey;
    }

    public void setMaxKey(int maxKey) {
        this.maxKey = maxKey;
    }

    public float getErrorRate() {
        return errorRate;
    }

    public void setErrorRate(float errorRate) {
        this.errorRate = errorRate;
    }

    public int getHashFunctionCount() {
        return hashFunctionCount;
    }

    public void setHashFunctionCount(int hashFunctionCount) {
        this.hashFunctionCount = hashFunctionCount;
    }

    public int getBitSize() {
        return bitSize;
    }

    public void setBitSize(int bitSize) {
        this.bitSize = bitSize;
    }
}
