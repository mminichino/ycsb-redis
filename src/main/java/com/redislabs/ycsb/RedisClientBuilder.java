package com.redislabs.ycsb;

import io.lettuce.core.ClientOptions;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.cluster.ClusterClientOptions;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.support.ConnectionPoolSupport;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import java.util.List;

public class RedisClientBuilder {

  private final RedisConfig redisConfig;
  private final ClientOptions clientOptions;
  private final ClusterClientOptions clusterClientOptions;
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

    clusterClientOptions = ClusterClientOptions.builder()
        .autoReconnect(true)
        .build();
  }

  public RedisURI getRedisURI() {
    return this.redisConfig.getRedisURI();
  }

  public RedisClient getClient() {
    RedisClient client = RedisClient.create(getRedisURI());
    client.setOptions(clientOptions);
    return client;
  }

  public RedisClusterClient getClusterClient() {
    List<RedisURI> uris = redisConfig.getClusterRedisURIs();
    RedisClusterClient client = RedisClusterClient.create(uris);
    client.setOptions(clusterClientOptions);
    return client;
  }

  public GenericObjectPool<StatefulRedisConnection<String, String>> getPool(RedisClient client, int poolMaxSize) {
    GenericObjectPoolConfig<StatefulRedisConnection<String, String>> poolConfig = new GenericObjectPoolConfig<>();
    configurePool(poolConfig, poolMaxSize);

    return ConnectionPoolSupport.createGenericObjectPool(
        client::connect,
        poolConfig
    );
  }

  public GenericObjectPool<StatefulRedisClusterConnection<String, String>> getClusterPool(
      RedisClusterClient client, int poolMaxSize) {
    GenericObjectPoolConfig<StatefulRedisClusterConnection<String, String>> poolConfig =
        new GenericObjectPoolConfig<>();
    configurePool(poolConfig, poolMaxSize);

    return ConnectionPoolSupport.createGenericObjectPool(
        client::connect,
        poolConfig
    );
  }

  private <T> void configurePool(GenericObjectPoolConfig<T> poolConfig, int poolMaxSize) {
    poolConfig.setMaxTotal(poolMaxSize);
    poolConfig.setMaxIdle(Math.min(maxIdle, poolMaxSize));
    poolConfig.setMinIdle(Math.min(minIdle, poolMaxSize));
    poolConfig.setTestOnBorrow(false);
    poolConfig.setTestOnReturn(false);
    poolConfig.setTestWhileIdle(true);
    poolConfig.setBlockWhenExhausted(true);
  }
}
