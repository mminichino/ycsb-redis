package com.redislabs.ycsb;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

class TestDeleteDatabase {
  public static final Logger logger = LoggerFactory.getLogger(TestDeleteDatabase.class);
  public static final String PROPERTY_FILE = "db.properties";

  @Test
  void runDeleteDatabase() {
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

    assertDoesNotThrow(() ->
        new DeleteDatabase().testClean(properties)
    );
  }
}
