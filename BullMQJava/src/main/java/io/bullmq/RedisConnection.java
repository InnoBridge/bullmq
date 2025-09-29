package io.bullmq;

import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisPooled;
import redis.clients.jedis.UnifiedJedis;

/**
 * Simple wrapper for a pooled Jedis connection.
 */
public class RedisConnection implements AutoCloseable {
    private final UnifiedJedis client;
    private final AutoCloseable closeAction;

    public RedisConnection(String host, int port, int database) {
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxTotal(16);
        poolConfig.setMaxIdle(16);
        poolConfig.setMinIdle(0);
        JedisPooled pooled = new JedisPooled(poolConfig, host, port, database);
        this.client = pooled;
        this.closeAction = pooled;
    }

    public RedisConnection(UnifiedJedis client) {
        this(client, () -> client.close());
    }

    public RedisConnection(UnifiedJedis client, AutoCloseable closeAction) {
        this.client = client;
        this.closeAction = closeAction;
    }

    public UnifiedJedis getClient() {
        return client;
    }

    @Override
    public void close() {
        try {
            if (closeAction != null) {
                closeAction.close();
            }
        } catch (Exception e) {
            throw new RuntimeException("Unable to close Redis connection", e);
        }
    }
}
