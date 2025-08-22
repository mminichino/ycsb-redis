package com.redislabs.ycsb;

import com.codelry.util.ycsb.TestCleanup;

import com.redis.lettucemod.RedisModulesClient;
import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.lettucemod.api.sync.RedisModulesCommands;
import io.lettuce.core.RedisURI;

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
      System.err.println("Error: " + e);
      e.printStackTrace(System.err);
      System.exit(1);
    }
  }

  public static void flushDatabase(Properties properties) {
    RedisConfig redisConfig = new RedisConfig(properties);

    RedisURI redisURI = redisConfig.getRedisURI();
    RedisModulesClient modulesClient = RedisModulesClient.create(redisURI);
    StatefulRedisModulesConnection<String, String> modulesConnection = modulesClient.connect();
    RedisModulesCommands<String, String> modulesCommands = modulesConnection.sync();

    try {
      logger.info("Flushing database");
      modulesCommands.flushdb();
    } catch (Exception e) {
      logger.error("Error flushing database: {}", e.getMessage(), e);
      System.exit(1);
    }
  }

  public RunClean() {
    super();
  }
}
