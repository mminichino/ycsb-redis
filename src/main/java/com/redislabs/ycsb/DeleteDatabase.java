package com.redislabs.ycsb;

import com.codelry.util.rest.REST;
import com.codelry.util.rest.exceptions.HttpResponseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Properties;

public final class DeleteDatabase {

  public static final Logger logger = LoggerFactory.getLogger(DeleteDatabase.class);

  public static void main(String[] args) {
    Properties properties = new Properties();

    try {
        deleteDatabase(properties);
    } catch (Exception e) {
      logger.error(e.getMessage(), e);
      System.exit(1);
    }
  }

  public static void deleteDatabase(Properties properties) {
    RedisConfig redisConfig = new RedisConfig(properties);
    int dBUid = redisConfig.getRedisEnterpriseDbUid();
    String hostname = redisConfig.getRedisEnterpriseApiHost();
    String username = redisConfig.getRedisEnterpriseUserName();
    String password = redisConfig.getRedisEnterprisePassword();
    int port = redisConfig.getRedisEnterpriseApiPort();
    boolean enterpriseDb = redisConfig.isEnterpriseDb();

    if (!enterpriseDb) {
      logger.info("Skipping database deletion on {}:{}", hostname, port);
      return;
    }

    REST client = new REST(hostname, username, password, true, port);

    logger.info("Deleting database on {}:{} as user {}", hostname, port, username);

    String dbDeleteEndpoint = String.format("/v1/bdbs/%d", dBUid);
    try {
      client.delete(dbDeleteEndpoint).validate();
    } catch (HttpResponseException e) {
      if (client.responseCode != 404) {
        logger.error("Error deleting database: response code: {} body: {}", client.responseCode, new String(client.responseBody, StandardCharsets.UTF_8));
        System.exit(1);
      } else {
        logger.info("Database does not exist");
      }
    }
  }

  public DeleteDatabase() {
    super();
  }
}
