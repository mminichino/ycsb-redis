package com.redislabs.ycsb;

import com.codelry.util.ycsb.RunBenchmark;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

class TestWorkloadB {

  @Test
  void runWorkloadB() {
    assertDoesNotThrow(() ->
        RunBenchmark.main(new String[] { "-w", "b" })
    );
  }
}
