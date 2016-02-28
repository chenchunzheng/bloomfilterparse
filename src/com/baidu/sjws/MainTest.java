package com.baidu.sjws;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import orestes.bloomfilter.BloomFilter;
import orestes.bloomfilter.FilterBuilder;
import orestes.bloomfilter.json.BloomFilterConverter;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.security.cert.CertificateEncodingException;
import java.util.Arrays;

public class MainTest {

    private static double FALSE_POSITIVE_PROBABILITY = 0.01;
    private static int  EXPECTED_ELEMENTS = 1000000;
    private static String SEPARATOR = ",";
    private static String COLON = ":";

    private static JsonObject getJsonBloomFilter(File file, String bloomName, String redishost, String redisPassword, boolean isRemove) throws IOException {
        //Open a Redis-backed Bloom filter
        FilterBuilder filterBuilder = new FilterBuilder(EXPECTED_ELEMENTS, FALSE_POSITIVE_PROBABILITY);
        filterBuilder.name(bloomName);
        filterBuilder.redisBacked(true);
        filterBuilder.overwriteIfExists(true);

        String[] hostList = redishost.split(SEPARATOR);
        for(String host: hostList) {
            String[] host_port = host.split(COLON);
            filterBuilder.redisHost(host_port[0]);
            filterBuilder.redisPort(Integer.valueOf(host_port[1]));
//            filterBuilder.addReadSlave(host_port[0], Integer.valueOf(host_port[1]));
        }

        if(StringUtils.isNotBlank(redisPassword)) {
            filterBuilder.password(redisPassword);
        }

        JsonObject jsonObject = new JsonObject();
        BloomFilter<String> bloomFilter = filterBuilder.buildBloomFilter();
        if(isRemove) {
            bloomFilter.remove();
            jsonObject.addProperty("isRemove", "success");
        }else {
            bloomFilter = initdata(file, bloomFilter);
            JsonElement json = BloomFilterConverter.toJson(bloomFilter);
            jsonObject  = json.getAsJsonObject();
        }

        jsonObject.addProperty("falsePositiveProbability", FALSE_POSITIVE_PROBABILITY);
        jsonObject.addProperty("expectedElements", EXPECTED_ELEMENTS);
        jsonObject.addProperty("bloomName", bloomName);
        return jsonObject;
    }

    private static BloomFilter<String> initdata(File file, BloomFilter<String> bloomFilter) {

        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new FileReader(file));
            String line;
            while ((line = reader.readLine()) != null) {
                line = StringUtils.stripToNull(line);
                if (null == line) {
                    continue;
                }
                bloomFilter.addAll(Arrays.asList(line.split(SEPARATOR)));
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            //file.delete();
            IOUtils.closeQuietly(reader);
        }
        return bloomFilter;
    }

    public static void main(String[] args) throws IOException, CertificateEncodingException {
        if(args.length < 4) {
            showUsage("usage: bloomfilterparse.jar [srcFile] [bloomName] [redishost] [redisPassword] [isRemove]");
            return;
        }
        String srcFile = args[0];
        String bloomName = args[1];
        String redishost = args[2];
        String redisPassword = args[3];
        String isRemove = args[4];

//        String srcFile = "/Users/baidu/Downloads/token.txt";
//        String bloomName = "bloom";
//        String redishost = "cp01-sjws-offline012.cp01:8769";
//        String redisPassword = "my_redis";
//        String isRemove = "true";

        File file = new File(srcFile);
        if(!file.exists()){
            showUsage("usage: file does not exist");
            return;
        }

        JsonObject jsonObject = getJsonBloomFilter(file, bloomName, redishost, redisPassword, Boolean.valueOf(isRemove));
        System.out.print(jsonObject);
    }

    public static void showUsage(String message) {
        System.out.println(message);
    }
}
