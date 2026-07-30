package com.redislabs.ycsb;

import com.codelry.util.ycsb.TestSetup;

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.Scanner;

public final class RunPrep extends TestSetup {

  public static final Logger logger = LoggerFactory.getLogger(RunPrep.class);

  @Override
  public void testSetup(Properties properties) {
    try {
      RedisConfig redisConfig = new RedisConfig(properties);
      requestConfirmation(redisConfig);
      testPrep(redisConfig);
    } catch (Exception e) {
      logger.error(e.getMessage(), e);
      System.exit(1);
    }
  }

  public void requestConfirmation(RedisConfig redisConfig) {
    if (System.getProperty("ycsb.run.confirm") != null || redisConfig.getConfirmationResponse().equals("yes")) {
      return;
    }

    Scanner scanner = new Scanner(System.in);

    logger.info("WARNING: This will flush all data in your Redis Database.");
    logger.info("Proceed with benchmark run? (y/n): ");
    String response = scanner.nextLine().toLowerCase();

    if (response.equals("y") || response.equals("yes")) {
      System.setProperty("ycsb.run.confirm", "true");
      scanner.close();
    } else {
      logger.warn("Benchmark run cancelled.");
      System.exit(0);
    }
  }

  public static void testPrep(RedisConfig redisConfig) {
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

  public RunPrep() {
    super();
  }
}
