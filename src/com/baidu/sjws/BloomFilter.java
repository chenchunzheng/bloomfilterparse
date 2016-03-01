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

    /**
     * expectedElements
     */
    private int n;
    /**
     * falsePositiveProbability
     */
    private float fpp;
    /**
     * hashFunctionCount
     */
    private int hCount;
    /**
     * bit size
     */
    private int bitSize;

    /**
     * bloom name
     */
    private String bloomName;

    /**
     * bit data
     */
    private String bits;

    public BloomFilter(int n, float p, String s) {
        this.n = n;
        fpp = p;
        bloomName = s;
        bitSize = BloomUtils.calcOptimalM(n, p);
        hCount = BloomUtils.calcOptimalK(bitSize, n);
    }

    public BloomFilter(int n, float p) {
        this(n, p, "");
    }

    public BloomFilter() {

    }

    public int getN() {
        return n;
    }

    public void setN(int n) {
        this.n = n;
    }

    public float getFpp() {
        return fpp;
    }

    public void setFpp(float fpp) {
        this.fpp = fpp;
    }

    public int gethCount() {
        return hCount;
    }

    public void sethCount(int hCount) {
        this.hCount = hCount;
    }

    public int getBitSize() {
        return bitSize;
    }

    public void setBitSize(int bitSize) {
        this.bitSize = bitSize;
    }

    public String getBloomName() {
        return bloomName;
    }

    public void setBloomName(String bloomName) {
        this.bloomName = bloomName;
    }

    public String getBits() {
        return bits;
    }

    public void setBits(String bits) {
        this.bits = bits;
    }
}