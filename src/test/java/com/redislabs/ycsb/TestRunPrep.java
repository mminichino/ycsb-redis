package com.redislabs.ycsb;

import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

class TestRunPrep {

  @Test
  void runCreateDatabase() {
    Properties properties = new Properties();

    assertDoesNotThrow(() ->
        new RunPrep().testSetup(properties)
    );
  }
}
