package com.baidu.sjws;

import com.dianxinos.jedis.wrapper.RedisService;
import com.google.gson.JsonObject;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

/**
 * Created by baidu on 16/2/26.
 */
public class Main {
    private static float FALSE_POSITIVE_PROBABILITY = 0.01f;
    private static int EXPECTED_ELEMENTS = 1000000;
    private static String SEPARATOR = ",";
    public static void main(String args[]) throws IOException {

        if(args.length < 6) {
            showUsage("usage: bloomfilterparse.jar [srcFile] [bloomName] [redishost] [redisPassword] [cachePrefix] [isRemove]");
            return;
        }
        String srcFile = args[0];
        String bloomName = args[1];
        String redishost = args[2];
        String redisPassword = args[3];
        String cachePrefix = args[4];
        String isRemove = args[5];

//        String srcFile = "/Users/baidu/Downloads/token.txt";
//        String bloomName = "bloom";
//        String redishost = "cp01-sjws-offline012.cp01:8769,cp01-sjws-offline012.cp01:8770";
//        String redisPassword = "my_redis";
//        String isRemove = "false";
//        String cachePrefix = "_newrec_";

        JsonObject jsonObject = new JsonObject();
        RedisService redisService = RedisUtils.getRedisService(redishost, redisPassword, cachePrefix);
        BloomFilter bloomFilter = new BloomFilter(EXPECTED_ELEMENTS, FALSE_POSITIVE_PROBABILITY, bloomName);
        if(Boolean.valueOf(isRemove)) {
            redisService.delete(bloomName);
            jsonObject.addProperty("isRemove", "success");
        }else {

            File file = new File(srcFile);
            if(!file.exists()){
                showUsage("usage: file does not exist");
                return;
            }

            String bitStr = initdata(file, redisService, bloomFilter);
            jsonObject.addProperty("fpp", FALSE_POSITIVE_PROBABILITY);
            jsonObject.addProperty("n", EXPECTED_ELEMENTS);
            jsonObject.addProperty("bloomName", bloomName);
            jsonObject.addProperty("bits", bitStr);
            jsonObject.addProperty("hCount", bloomFilter.gethCount());
            jsonObject.addProperty("bitSize", bloomFilter.getBitSize());

        }
        System.out.print(jsonObject);
    }

    private static String initdata(File file, RedisService redisService, BloomFilter bloomFilter) throws IOException {

        BufferedReader reader = null;
        String bloomName = bloomFilter.getBloomName();
        try {
            reader = new BufferedReader(new FileReader(file));
            String line;
            while ((line = reader.readLine()) != null) {
                line = StringUtils.stripToNull(line);
                if (null == line) {
                    continue;
                }
                String[] token_imeis = line.split(SEPARATOR);
                for(String token_imei:token_imeis) {
                   long[] offset = BloomUtils.murmurHashOffset(token_imei, bloomFilter.gethCount(), bloomFilter.getBitSize());
                   redisService.setMultiBit(bloomName, offset, null);
                   //List<Object> valueExits = redisService.getMultiBit(bloomName, offset);
                }

            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            //file.delete();
            IOUtils.closeQuietly(reader);
        }

        byte[] redisServiceValue = redisService.get(bloomName);
        String base64String = Base64.encodeBase64String(redisServiceValue);

//        test exists
//        String testTk = "0000c3UFGH0C2VKaMEjh0A==";
//        long[] offset = BloomUtils.murmurHashOffset(testTk, bloomFilter.gethCount(), bloomFilter.getBitSize());
//        List<Object> value = redisService.getMultiBit(bloomName, offset);
//        //test restore
//        redisService.delete(bloomName);
//        byte[] storedBytes = Base64.decodeBase64(base64String);
//        redisService.set(bloomName, storedBytes);
//        value = redisService.getMultiBit(bloomName, offset);

        return base64String;
    }

    public static void showUsage(String message) {
        System.out.println(message);
    }
}
