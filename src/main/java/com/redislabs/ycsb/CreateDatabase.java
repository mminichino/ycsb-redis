package com.redislabs.ycsb;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

import com.codelry.util.rest.REST;
import com.codelry.util.rest.exceptions.HttpResponseException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public final class CreateDatabase {

  private static final Logger logger = LoggerFactory.getLogger(CreateDatabase.class);

  private static final String PROPERTY_FILE = "db.properties";

  private static final ObjectMapper mapper = new ObjectMapper();

  public static void main(String[] args) {
    ClassLoader classloader = RedisClientBinding.class.getClassLoader();
    Properties properties = new Properties();

    try (InputStream in = classloader.getResourceAsStream(PROPERTY_FILE)) {
      if (in != null) {
        logger.debug("Loading properties from resource {}", PROPERTY_FILE);
        properties.load(in);
      } else {
        logger.warn("Resource {} not found on classpath", PROPERTY_FILE);
      }
    } catch (IOException e) {
      logger.error("Error loading properties: {}", e.getMessage(), e);
      System.exit(1);
    }

    try {
      createDatabase(properties);
    } catch (Exception e) {
      System.err.println("Error: " + e);
      e.printStackTrace(System.err);
      System.exit(1);
    }
  }

  public static void createDatabase(Properties properties) {
    RedisConfig redisConfig = new RedisConfig(properties);
    String hostname = redisConfig.getRedisHost();
    String username = redisConfig.getRedisEnterpriseUserName();
    String password = redisConfig.getRedisEnterprisePassword();
    int port = redisConfig.getRedisEnterpriseApiPort();
    int memory = redisConfig.getRedisEnterpriseMemory();
    int shards = redisConfig.getRedisEnterpriseShards();
    REST client = new REST(hostname, username, password, true, port);

    logger.info("Creating database on {}:{} as user {}", hostname, port, username);

    String endpoint = "/v1/bdbs";
    ObjectNode body = getSettings(memory, shards);
    try {
      client.post(endpoint, body).validate().json();
      String dbGetEndpoint = "/v1/bdbs/1";
      if (!client.waitForJsonValue(dbGetEndpoint, "status", "active", 120)) {
        throw new RuntimeException("timeout waiting for database creation to complete");
      }
    } catch (HttpResponseException e) {
      logger.error("Error creating database: response code: {} body: {}",
          client.responseCode,
          new String(client.responseBody, StandardCharsets.UTF_8));
      System.exit(1);
    }
  }

  private static ObjectNode getSettings(int memory, int shards) {
    ObjectNode body = mapper.createObjectNode();

    body.put("memory_size", memory);
    body.put("name", "ycsb");
    body.put("port", 12000);
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

  private CreateDatabase() {
    super();
  }
}
