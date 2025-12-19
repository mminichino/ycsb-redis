package com.redislabs.ycsb;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;
import org.junit.jupiter.api.BeforeAll;

public abstract class RedisTest {

  static final GenericContainer<?> REDIS_CONTAINER =
      new GenericContainer(DockerImageName.parse("redis:latest")) {
        {
          addFixedExposedPort(6379, 6379);
        }
      };

  @BeforeAll
  static void startContainer() {
    if (!REDIS_CONTAINER.isRunning()) {
      REDIS_CONTAINER.start();
    }
  }
}
