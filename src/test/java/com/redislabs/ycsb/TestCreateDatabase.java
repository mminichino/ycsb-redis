package com.redislabs.ycsb;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

class TestCreateDatabase {

  @Test
  void runCreateDatabase() {
    assertDoesNotThrow(() ->
        CreateDatabase.main(new String[0])
    );
  }
}
