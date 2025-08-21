package com.redislabs.ycsb;

import com.codelry.util.rest.REST;
import com.codelry.util.rest.exceptions.HttpResponseException;

import com.codelry.util.ycsb.TestCleanup;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Properties;


public final class DeleteDatabase extends TestCleanup {

  public static final Logger logger = LoggerFactory.getLogger(DeleteDatabase.class);

  @Override
  public void testClean(Properties properties) {
    try {
      deleteDatabase(properties);
    } catch (Exception e) {
      System.err.println("Error: " + e);
      e.printStackTrace(System.err);
      System.exit(1);
    }
  }

  public static void deleteDatabase(Properties properties) {
    RedisConfig redisConfig = new RedisConfig(properties);
    String hostname = redisConfig.getRedisHost();
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

    String endpoint = "/v1/bdbs/1";
    try {
      client.delete(endpoint).validate();
    } catch (HttpResponseException e) {
      if (client.responseCode == 404) return;
      logger.error("Error deleting database: response code: {} body: {}",
          client.responseCode,
          new String(client.responseBody, StandardCharsets.UTF_8));
      System.exit(1);
    }
  }

  public DeleteDatabase() {
    super();
  }
}
