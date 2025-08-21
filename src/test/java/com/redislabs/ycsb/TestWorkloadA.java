package com.redislabs.ycsb;

import com.codelry.util.ycsb.RunBenchmark;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

class TestWorkloadA {

  @Test
  void runWorkloadA() {
    assertDoesNotThrow(() ->
        RunBenchmark.main(new String[] { "-w", "a" })
    );
  }
}
