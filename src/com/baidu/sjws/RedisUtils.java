package com.baidu.sjws;

import com.dianxinos.jedis.wrapper.RedisService;
import com.dianxinos.jedis.wrapper.ShardedRedisServiceImpl;
import com.dianxinos.jedis.wrapper.serializer.JacksonJsonRedisSerializer;
import com.dianxinos.jedis.wrapper.serializer.RedisSerializer;
import org.apache.commons.lang.StringUtils;
import org.codehaus.jackson.map.ObjectMapper;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisShardInfo;
import redis.clients.jedis.ShardedJedisPool;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by baidu on 16/2/26.
 */
public class RedisUtils {

    private static int MAX_TOTAL = 100;
    private static int MAX_IDEL = 10;
    private static int TIME_OUT = 5000;

    public static RedisService getRedisService (String redishost, String redisPassword, String cachePrefix) {
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxTotal(MAX_TOTAL);
        poolConfig.setMaxIdle(MAX_IDEL);
        List<JedisShardInfo> shards = new ArrayList<JedisShardInfo>();
        String[] shardedAddress = redishost.split(",");
        for (String address : shardedAddress) {
            InetSocketAddress inetSocketAddress = getAddress(address);
            JedisShardInfo si = new JedisShardInfo(inetSocketAddress.getAddress().getHostAddress(), inetSocketAddress.getPort(), TIME_OUT);
            if (StringUtils.isNotBlank(redisPassword)) {
                si.setPassword(redisPassword);
            }
            shards.add(si);
        }

        ShardedJedisPool pool = new ShardedJedisPool(poolConfig, shards);
        RedisSerializer redisSerializer = new JacksonJsonRedisSerializer(new ObjectMapper());
        RedisService redisService = new ShardedRedisServiceImpl(pool, redisSerializer, cachePrefix);

        return redisService;
    }

    private static InetSocketAddress getAddress(String address) {
        int finalColon = address.lastIndexOf(':');
        if (finalColon < 1) {
            throw new IllegalArgumentException("Invalid address:"
                    + address);

        }
        String hostPart = address.substring(0, finalColon);
        String portNum = address.substring(finalColon + 1);
        return new InetSocketAddress(hostPart, Integer.parseInt(portNum));
    }
}
