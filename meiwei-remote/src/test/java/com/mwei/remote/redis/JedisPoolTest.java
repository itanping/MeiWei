package com.mwei.remote.redis;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisSentinelPool;

/**
 * @author tanping
 * @date 2019/9/9 11:16
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration("classpath:testApplicationContext.xml")
public class JedisPoolTest {

    @Autowired
    private JedisSentinelPool jedisPool;

    @Test
    public void testJedisPool4Get() {
        Jedis jedis = null;

        try {
            jedis = jedisPool.getResource();
            jedis.set("uid_010_name", "美味物语");
            System.out.println("uid_010_name : " + jedis.get("uid_010_name"));
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }
}
