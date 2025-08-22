package com.redislabs.ycsb;

import com.codelry.util.ycsb.TestSetup;

import com.redis.lettucemod.RedisModulesClient;
import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.lettucemod.api.sync.RedisModulesCommands;
import com.redis.lettucemod.search.CreateOptions;
import com.redis.lettucemod.search.Field;
import com.redis.lettucemod.search.NumericField;
import io.lettuce.core.RedisCommandExecutionException;
import io.lettuce.core.RedisURI;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public final class RunPrep extends TestSetup {

  public static final Logger logger = LoggerFactory.getLogger(RunPrep.class);

  @Override
  public void testSetup(Properties properties) {
    try {
      testPrep(properties);
    } catch (Exception e) {
      logger.error(e.getMessage(), e);
      System.exit(1);
    }
  }

  public static void testPrep(Properties properties) {
    RedisConfig redisConfig = new RedisConfig(properties);
    boolean enterpriseDb = redisConfig.isEnterpriseDb();

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

    if (!enterpriseDb) {
      logger.info("Skipping index creation");
      return;
    }

    try {
      if (searchStrategy.equals("JSON")) {
        NumericField<String> idJsonField = Field.numeric("$.id").as("id").build();

        CreateOptions<String, String> optionsJson = CreateOptions.<String, String>builder()
            .on(CreateOptions.DataType.JSON)
            .prefix("user")
            .build();

        String resultJson = ftCreateSafe(modulesCommands, jsonIndexName, optionsJson, idJsonField);
        if (resultJson.equals("OK")) {
          logger.info("JSON Index {} created", jsonIndexName);
        } else {
          logger.error("Error creating json index {}: {}", jsonIndexName, resultJson);
          System.exit(1);
        }
      } else if (searchStrategy.equals("HASH")) {
        NumericField<String> idHashField = Field.numeric("id").as("id").build();

        CreateOptions<String, String> optionsHash = CreateOptions.<String, String>builder()
            .on(CreateOptions.DataType.HASH)
            .prefix("user")
            .build();

        String resultHash = ftCreateSafe(modulesCommands, hashIndexName, optionsHash, idHashField);
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

  @SafeVarargs
  private static String ftCreateSafe(RedisModulesCommands<String, String> commands,
                                          String index,
                                          CreateOptions<String, String> options,
                                          Field<String>... fields) {
    return commands.ftCreate(index, options, fields);
  }

  public RunPrep() {
    super();
  }
}
