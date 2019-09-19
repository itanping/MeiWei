package com.meiwei.service.redis;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisSentinelPool;

import java.util.HashSet;
import java.util.Set;

/**
 * @author tanping
 * @date 2019/9/9 12:48
 */
public class JedisUtil {

    private static JedisSentinelPool jedisPool = null;

    // 自带的哨兵模式 JedisSentinelPool, 并在一开始初始化连接池
    static {
        try {
            JedisPoolConfig config = new JedisPoolConfig();
            // 资源池中的最大连接数
            // 如果赋值为-1，则表示不限制；如果pool已经分配了maxActive个jedis实例，则此时pool的状态为exhausted（耗尽）
            config.setMaxTotal(Integer.valueOf(50));
            // 控制一个pool最多有多少个状态为idle（空闲的）的jedis实例
            config.setMaxIdle(Integer.valueOf(50));
            // 表示当borrow（引入）一个jedis实例时，最大的等待时间，如果超过等待时间，则直接抛出JedisConnectionException
            config.setMinEvictableIdleTimeMillis(Integer.valueOf(-1));
            // 在borrow一个jedis实例时，是否提前进行validate操作；如果为true，则得到的jedis实例均是可用的
            config.setTestOnBorrow(Boolean.valueOf(true));

            // Setinel哨兵群
            // Setinel客户端提供了master自动发现功能
            Set<String> sentinels = new HashSet<>();
            sentinels.add("127.0.0.1:26379");
            sentinels.add("127.0.0.1:26380");
            sentinels.add("127.0.0.1:26381");

            // master名称必须要和配置文件sentinel.conf中配置保持一致
            jedisPool = new JedisSentinelPool("mymaster", sentinels, config);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 构建redis连接池
     *
     * @return JedisPool
     */
    public static JedisSentinelPool getJedisPool() {
        return jedisPool;
    }

    /**
     * 测试redis线程池是否正常
     *
     * @param args
     */
    public static void main(String[] args) {
        JedisSentinelPool jedisSentinelPool = JedisUtil.getJedisPool();
        Jedis jedis = jedisSentinelPool.getResource();
        System.out.println("jedis = " + jedis);

        jedis.set("uid_020_name", "美味进击");
        System.out.println("uid_020_name : " + jedis.get("uid_020_name"));

        // Jedis 2.9.0 版本及以上的 JedisPool 的 returnBrokenResource() 和 returnResource() 方法被标注废弃了，取而代之的是 Jedis 的 close()
        if (jedis != null) {
            jedis.close();
        }
    }
}
