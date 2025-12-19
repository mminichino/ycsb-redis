package com.redislabs.ycsb;

import com.codelry.util.ycsb.RunBenchmark;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

class TestWorkloadAll extends RedisTest {

  @Test
  void runWorkloadA() {
    assertDoesNotThrow(() ->
        RunBenchmark.main(new String[] {})
    );
  }
}
