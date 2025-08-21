package com.redislabs.ycsb;

import com.codelry.util.ycsb.TestSetup;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.nio.charset.StandardCharsets;
import java.util.Properties;

import com.codelry.util.rest.REST;
import com.codelry.util.rest.exceptions.HttpResponseException;

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


public final class CreateDatabase extends TestSetup {

  public static final Logger logger = LoggerFactory.getLogger(CreateDatabase.class);

  public static final ObjectMapper mapper = new ObjectMapper();

  @Override
  public void testSetup(Properties properties) {
    try {
      createDatabase(properties);
      createIndex(properties);
    } catch (Exception e) {
      logger.error(e.getMessage(), e);
      System.exit(1);
    }
  }

  public static void createDatabase(Properties properties) {
    RedisConfig redisConfig = new RedisConfig(properties);
    String hostname = redisConfig.getRedisHost();
    String username = redisConfig.getRedisEnterpriseUserName();
    String password = redisConfig.getRedisEnterprisePassword();
    int port = redisConfig.getRedisEnterpriseApiPort();
    int dbPort = redisConfig.getRedisPort();
    int memory = redisConfig.getRedisEnterpriseMemory();
    int shards = redisConfig.getRedisEnterpriseShards();
    boolean enterpriseDb = redisConfig.isEnterpriseDb();

    if (!enterpriseDb) {
      logger.info("Skipping database creation on {}:{}", hostname, port);
      return;
    }

    REST client = new REST(hostname, username, password, true, port);

    logger.info("Creating database on {}:{} as user {}", hostname, port, username);

    String endpoint = "/v1/bdbs";
    ObjectNode body = getSettings(dbPort, memory, shards);
    try {
      client.post(endpoint, body).validate().json();
      String dbGetEndpoint = "/v1/bdbs/1";
      if (!client.waitForJsonValue(dbGetEndpoint, "status", "active", 120)) {
        throw new RuntimeException("timeout waiting for database creation to complete");
      }
    } catch (HttpResponseException e) {
      if (client.responseCode == 409) return;
      logger.error("Error creating database: response code: {} body: {}",
          client.responseCode,
          new String(client.responseBody, StandardCharsets.UTF_8));
      System.exit(1);
    }
  }

  public static void createIndex(Properties properties) {
    RedisConfig redisConfig = new RedisConfig(properties);
    boolean enterpriseDb = redisConfig.isEnterpriseDb();

    if (!enterpriseDb) {
      logger.info("Skipping index creation");
      return;
    }

    RedisURI redisURI = redisConfig.getRedisURI();
    RedisModulesClient modulesClient = RedisModulesClient.create(redisURI);
    StatefulRedisModulesConnection<String, String> modulesConnection = modulesClient.connect();
    RedisModulesCommands<String, String> modulesCommands = modulesConnection.sync();
    String searchStrategy = redisConfig.getSearchStrategy();
    String jsonIndexName = redisConfig.getIndexJson();
    String hashIndexName = redisConfig.getIndexHash();

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

  public static ObjectNode getSettings(int port, int memory, int shards) {
    ObjectNode body = mapper.createObjectNode();

    body.put("memory_size", memory);
    body.put("name", "ycsb");
    body.put("port", port);
    body.put("proxy_policy", "all-nodes");
    body.put("shards_count", shards);
    body.put("type", "redis");
    body.put("uid", 1);

    ArrayNode modulesList = new ArrayNode(mapper.getNodeFactory());
    ObjectNode search = mapper.createObjectNode();
    search.put("module_name", "search");
    search.put("module_args", "WORKERS 6");
    modulesList.add(search);
    ObjectNode json = mapper.createObjectNode();
    json.put("module_name", "ReJSON");
    json.put("module_args", "");
    modulesList.add(json);
    body.set("module_list", modulesList);
    body.put("sched_policy", "mnp");
    body.put("conns", 32);

    if (shards > 1) {
      body.put("sharding", true);
      ArrayNode shardKeyRegex = new ArrayNode(mapper.getNodeFactory());
      ObjectNode withHashTag = mapper.createObjectNode();
      withHashTag.put("regex", ".*\\{(?<tag>.*)\\}.*");
      ObjectNode withoutHashTag = mapper.createObjectNode();
      withoutHashTag.put("regex", "(?<tag>.*)");
      shardKeyRegex.add(withHashTag);
      shardKeyRegex.add(withoutHashTag);
      body.set("shard_key_regex", shardKeyRegex);
    }

    return body;
  }

  public CreateDatabase() {
    super();
  }
}
