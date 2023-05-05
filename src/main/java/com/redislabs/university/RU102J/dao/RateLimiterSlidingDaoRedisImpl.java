package com.redislabs.university.RU102J.dao;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Response;
import redis.clients.jedis.Transaction;

import java.time.Instant;
import java.util.Random;

public class RateLimiterSlidingDaoRedisImpl implements RateLimiter {

    private final JedisPool jedisPool;
    private final long windowSizeMS;
    private final long maxHits;

    public RateLimiterSlidingDaoRedisImpl(JedisPool pool, long windowSizeMS,
                                          long maxHits) {
        this.jedisPool = pool;
        this.windowSizeMS = windowSizeMS;
        this.maxHits = maxHits;
    }

    // Challenge #7
    @Override
    public void hit(String name) throws RateLimitExceededException {
        try(Jedis jedis = jedisPool.getResource()) {
            String key = RedisSchema.getRateLimiterSlidingKey(windowSizeMS, name, maxHits);
            long timestamp = Instant.now().toEpochMilli();
            try(Transaction transaction = jedis.multi()) {
                String member = timestamp + "-" + new Random().nextDouble();
                transaction.zadd(key, (double) timestamp, member);
                transaction.zremrangeByScore(key, 0, timestamp - windowSizeMS);
                Response<Long> hits = transaction.zcard(key);
                transaction.exec();
                if(hits.get() > maxHits) {
                    throw new RateLimitExceededException();
                }
            }
        }
    }
}
