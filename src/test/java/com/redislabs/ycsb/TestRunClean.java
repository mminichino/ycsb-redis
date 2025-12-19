package com.redislabs.ycsb;

import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

class TestRunClean extends RedisTest {

  @Test
  void runTestClean() {
    Properties properties = new Properties();

    assertDoesNotThrow(() ->
        new RunClean().testClean(properties)
    );
  }
}
