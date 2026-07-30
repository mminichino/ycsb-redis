package com.redislabs.ycsb;

import com.codelry.util.ycsb.TestCleanup;

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public final class RunClean extends TestCleanup {

  public static final Logger logger = LoggerFactory.getLogger(RunClean.class);

  @Override
  public void testClean(Properties properties) {
    try {
      flushDatabase(properties);
    } catch (Exception e) {
      logger.error("Error: {}", String.valueOf(e));
      e.printStackTrace(System.err);
      System.exit(1);
    }
  }

  public static void flushDatabase(Properties properties) {
    RedisConfig redisConfig = new RedisConfig(properties);

    try {
      logger.info("Flushing database");
      if (redisConfig.isOssClusterEnabled()) {
        RedisClusterClient client = RedisClusterClient.create(redisConfig.getClusterRedisURIs());
        try (StatefulRedisClusterConnection<String, String> connection = client.connect()) {
          connection.sync().flushall();
        } finally {
          client.shutdown();
        }
      } else {
        RedisClient client = RedisClient.create(redisConfig.getRedisURI());
        try (StatefulRedisConnection<String, String> connection = client.connect()) {
          connection.sync().flushdb();
        } finally {
          client.shutdown();
        }
      }
    } catch (Exception e) {
      logger.error("Error flushing database: {}", e.getMessage(), e);
      System.exit(1);
    }
  }

  public RunClean() {
    super();
  }
}
