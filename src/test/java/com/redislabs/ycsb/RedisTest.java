package com.redislabs.ycsb;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

public abstract class RedisTest {

  protected GenericContainer<?> redis;

  @BeforeEach
  void startRedis() {
    redis = new GenericContainer<>(DockerImageName.parse("redis:8.6"))
        .withExposedPorts(6379);
    redis.start();
    System.setProperty(RedisConfig.REDIS_HOST, redis.getHost());
    System.setProperty(RedisConfig.REDIS_PORT, String.valueOf(redis.getMappedPort(6379)));
  }

  @AfterEach
  void stopRedis() {
    System.clearProperty(RedisConfig.REDIS_HOST);
    System.clearProperty(RedisConfig.REDIS_PORT);
    if (redis != null) {
      redis.stop();
      redis = null;
    }
  }
}
