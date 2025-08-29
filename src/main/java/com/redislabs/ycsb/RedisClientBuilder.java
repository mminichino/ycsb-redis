package com.redislabs.ycsb;

import com.redis.lettucemod.RedisModulesClient;
import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import io.lettuce.core.ClientOptions;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.support.ConnectionPoolSupport;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

public class RedisClientBuilder {

  private final RedisConfig redisConfig;
  private final ClientOptions clientOptions;
  private final int maxIdle;
  private final int minIdle;

  public RedisConfig getRedisConfig() {
    return redisConfig;
  }

  public static class Builder {

    private RedisConfig redisConfig;
    private int maxIdle = 4;
    private int minIdle = 2;

    public Builder redisConfig(RedisConfig redisConfig) {
      this.redisConfig = redisConfig;
      return this;
    }

    public Builder maxIdle(int maxIdle) {
      this.maxIdle = maxIdle;
      return this;
    }

    public Builder minIdle(int minIdle) {
      this.minIdle = minIdle;
      return this;
    }

    public RedisClientBuilder build() {
      return new RedisClientBuilder(this);
    }
  }

  RedisClientBuilder(Builder builder) {
    this.redisConfig = builder.redisConfig;
    this.maxIdle = builder.maxIdle;
    this.minIdle = builder.minIdle;

    clientOptions = ClientOptions.builder()
        .autoReconnect(true)
        .build();
  }

  public RedisURI getRedisURI() {
    return this.redisConfig.getRedisURI();
  }

  public RedisModulesClient getModulesClient() {
    RedisModulesClient client = RedisModulesClient.create(getRedisURI());
    client.setOptions(clientOptions);
    return client;
  }

  public RedisClient getClient() {
    RedisClient client = RedisClient.create(getRedisURI());
    client.setOptions(clientOptions);
    return client;
  }

  public GenericObjectPool<StatefulRedisModulesConnection<String, String>> getModulesPool(RedisModulesClient client, int poolMaxSize) {
    GenericObjectPoolConfig<StatefulRedisModulesConnection<String, String>> poolConfig = new GenericObjectPoolConfig<>();

    poolConfig.setMaxTotal(poolMaxSize);
    poolConfig.setMaxIdle(maxIdle);
    poolConfig.setMinIdle(minIdle);
    poolConfig.setTestOnBorrow(true);
    poolConfig.setTestOnReturn(true);
    poolConfig.setTestWhileIdle(true);
    poolConfig.setBlockWhenExhausted(true);

    return ConnectionPoolSupport.createGenericObjectPool(
        client::connect,
        poolConfig
    );
  }

  public GenericObjectPool<StatefulRedisConnection<String, String>> getPool(RedisClient client, int poolMaxSize) {
    GenericObjectPoolConfig<StatefulRedisConnection<String, String>> poolConfig = new GenericObjectPoolConfig<>();

    poolConfig.setMaxTotal(poolMaxSize);
    poolConfig.setMaxIdle(maxIdle);
    poolConfig.setMinIdle(minIdle);
    poolConfig.setTestOnBorrow(true);
    poolConfig.setTestOnReturn(true);
    poolConfig.setTestWhileIdle(true);
    poolConfig.setBlockWhenExhausted(true);

    return ConnectionPoolSupport.createGenericObjectPool(
        client::connect,
        poolConfig
    );
  }
}
