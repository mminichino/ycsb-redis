package com.redislabs.ycsb;

import com.codelry.util.ycsb.TestSetup;

import com.redis.lettucemod.RedisModulesClient;
import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.lettucemod.api.sync.RedisModulesCommands;
import io.lettuce.core.search.arguments.CreateArgs;
import io.lettuce.core.search.arguments.FieldArgs;
import io.lettuce.core.search.arguments.NumericFieldArgs;
import io.lettuce.core.RedisCommandExecutionException;
import io.lettuce.core.RedisURI;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

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

    System.out.println("WARNING: This will flush all data in your Redis Database.");
    System.out.print("Proceed with benchmark run? (y/n): ");
    String response = scanner.nextLine().toLowerCase();

    if (response.equals("y") || response.equals("yes")) {
      System.out.println();
      System.setProperty("ycsb.run.confirm", "true");
      scanner.close();
    } else {
      System.out.println("Benchmark run cancelled.");
      System.exit(0);
    }
  }

  public static void testPrep(RedisConfig redisConfig) {
    RedisURI redisURI = redisConfig.getRedisURI();
    RedisModulesClient modulesClient = RedisModulesClient.create(redisURI);
    StatefulRedisModulesConnection<String, String> modulesConnection = modulesClient.connect();
    RedisModulesCommands<String, String> modulesCommands = modulesConnection.sync();
    String searchStrategy = redisConfig.getSearchStrategy();
    String jsonIndexName = redisConfig.getIndexJson();
    String hashIndexName = redisConfig.getIndexHash();

    try {
      logger.info("Flushing database");
      modulesCommands.flushdb();
    } catch (Exception e) {
      logger.error("Error flushing database: {}", e.getMessage(), e);
      System.exit(1);
    }

    try {
      if (searchStrategy.equals("JSON")) {
        List<FieldArgs<String>> schema = Collections.singletonList(
            NumericFieldArgs.<String>builder().name("$.id").as("id").build()
        );

        CreateArgs<String, String> createArgs = CreateArgs.<String, String>builder()
            .on(CreateArgs.TargetType.JSON)
            .withPrefix("user")
            .build();

        String resultJson = modulesCommands.ftCreate(jsonIndexName, createArgs, schema);
        if (resultJson.equals("OK")) {
          logger.info("JSON Index {} created", jsonIndexName);
        } else {
          logger.error("Error creating json index {}: {}", jsonIndexName, resultJson);
          System.exit(1);
        }
      } else if (searchStrategy.equals("HASH")) {
        List<FieldArgs<String>> schema = Collections.singletonList(
            NumericFieldArgs.<String>builder().name("id").as("id").build()
        );

        CreateArgs<String, String> createArgs = CreateArgs.<String, String>builder()
            .on(CreateArgs.TargetType.HASH)
            .withPrefix("user")
            .build();

        String resultHash = modulesCommands.ftCreate(hashIndexName, createArgs, schema);
        if (resultHash.equals("OK")) {
          logger.info("Hash Index {} created", hashIndexName);
        } else {
          logger.error("Error creating hash index {}: {}", hashIndexName, resultHash);
          System.exit(1);
        }
      }
    } catch (RedisCommandExecutionException r) {
      if (r.getMessage().contains("Index already exists")) return;
      logger.error("Redis error creating index: {}", r.getMessage(), r);
      System.exit(1);
    } catch (Exception e) {
      logger.error("Error creating index: {}", e.getMessage(), e);
      System.exit(1);
    }
  }

  public RunPrep() {
    super();
  }
}
