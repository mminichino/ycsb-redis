package com.redislabs.ycsb;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.Properties;
import java.util.StringJoiner;

import com.codelry.util.rest.REST;
import com.codelry.util.rest.exceptions.HttpResponseException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class CreateDatabase {

  public static final Logger logger = LoggerFactory.getLogger(CreateDatabase.class);

  public static final ObjectMapper mapper = new ObjectMapper();

  public static void main(String[] args) {
    Properties properties = new Properties();

    try {
      createDatabase(properties);
    } catch (Exception e) {
      logger.error(e.getMessage(), e);
      System.exit(1);
    }
  }

  public static void createDatabase(Properties properties) {
    RedisConfig redisConfig = new RedisConfig(properties);
    String hostname = redisConfig.getRedisEnterpriseApiHost();
    String username = redisConfig.getRedisEnterpriseUserName();
    String password = redisConfig.getRedisEnterprisePassword();
    String persistence = redisConfig.getDataPersistence();
    int port = redisConfig.getRedisEnterpriseApiPort();
    int dbPort = redisConfig.getRedisPort();
    long memory = redisConfig.getRedisEnterpriseMemory();
    int shards = redisConfig.getRedisEnterpriseShards();
    boolean enterpriseDb = redisConfig.isEnterpriseDb();

    if (!enterpriseDb) {
      logger.info("Skipping database creation on {}:{}", hostname, port);
      return;
    }

    REST client = new REST(hostname, username, password, true, port);

    logger.info("Creating database on {}:{} as user {}", hostname, port, username);

    String endpoint = "/v1/bdbs";
    String dbGetEndpoint = "/v1/bdbs/1";
    ObjectNode body = getSettings(dbPort, memory, shards, persistence);
    try {
      client.post(endpoint, body).validate().json();
      if (!client.waitForJsonValue(dbGetEndpoint, "status", "active", 120)) {
        throw new RuntimeException("timeout waiting for database creation to complete");
      }
    } catch (HttpResponseException e) {
      if (client.responseCode != 409) {
        logger.error("Error creating database: response code: {} body: {}", client.responseCode, new String(client.responseBody, StandardCharsets.UTF_8));
        System.exit(1);
      } else {
        logger.info("Database already exists");
      }
    }

    try {
      JsonNode db = client.get(dbGetEndpoint).validate().json();
      for (Iterator<JsonNode> it = db.withArrayProperty("endpoints").elements(); it.hasNext(); ) {
        JsonNode node = it.next();
        StringJoiner addresses = new StringJoiner(",");
        for (Iterator<JsonNode> addrs = node.withArrayProperty("addr").elements(); addrs.hasNext(); ) {
          JsonNode addr = addrs.next();
          addresses.add(addr.asText());
        }
        String dnsName = node.get("dns_name").asText();
        String endpointPort = node.get("port").asText();
        logger.info("Endpoint: {}:{} ({})", dnsName, endpointPort, addresses);
      }
    } catch (HttpResponseException e) {
      logger.error("Error getting database endpoints: response code: {} body: {}", client.responseCode, new String(client.responseBody, StandardCharsets.UTF_8));
      System.exit(1);
    }
  }

  public static ObjectNode getSettings(int port, long memory, int shards, String persistence) {
    ObjectNode body = mapper.createObjectNode();

    body.put("memory_size", memory);
    body.put("name", "ycsb");
    body.put("port", port);
    body.put("proxy_policy", "all-nodes");
    body.put("shards_count", shards);
    body.put("type", "redis");
    body.put("uid", 1);

    switch (persistence) {
      case "AOF":
        body.put("data_persistence", "aof");
        break;
      case "SNAPSHOT":
        body.put("data_persistence", "snapshot");
        ArrayNode snapshotPolicy = new ArrayNode(mapper.getNodeFactory());
        ObjectNode policy = mapper.createObjectNode();
        policy.put("secs", 3600);
        policy.put("writes", 1);
        snapshotPolicy.add(policy);
        body.set("snapshot_policy", snapshotPolicy);
        break;
      default:
        body.put("data_persistence", "disabled");
        break;
    }

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
