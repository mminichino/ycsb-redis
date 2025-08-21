package com.redislabs.ycsb;

import io.lettuce.core.RedisURI;
import io.lettuce.core.StaticCredentialsProvider;

import java.util.Properties;

public class RedisConfig {
  private String redisHost;
  private String redisPassword;
  private String redisUsername;
  private int redisPort;
  private int redisDatabase;
  private boolean sslEnabled;

  private boolean enterpriseDb;
  private String redisEnterpriseUserName;
  private String redisEnterprisePassword;
  private int redisEnterpriseApiPort;
  private int redisEnterpriseShards;
  private int redisEnterpriseMemory;

  public static final String REDIS_HOST = "redis.host";
  public static final String REDIS_PORT = "redis.port";
  public static final String REDIS_USERNAME = "redis.username";
  public static final String REDIS_PASSWORD = "redis.password";
  public static final String REDIS_DATABASE = "redis.database";
  public static final String REDIS_SSL = "redis.ssl";

  public static final String REDIS_ENTERPRISE = "redis.enterprise";
  public static final String REDIS_ENTERPRISE_USERNAME = "redis.enterprise.username";
  public static final String REDIS_ENTERPRISE_PASSWORD = "redis.enterprise.password";
  public static final String REDIS_ENTERPRISE_API_PORT = "redis.enterprise.api.port";
  public static final String REDIS_ENTERPRISE_SHARDS = "redis.enterprise.shards";
  public static final String REDIS_ENTERPRISE_MEMORY = "redis.enterprise.memory";

  public static final String REDIS_HOST_ENV_VAR = "REDIS_HOST";
  public static final String REDIS_PORT_ENV_VAR = "REDIS_PORT";
  public static final String REDIS_USERNAME_ENV_VAR = "REDIS_USERNAME";
  public static final String REDIS_PASSWORD_ENV_VAR = "REDIS_PASSWORD";
  public static final String REDIS_DATABASE_ENV_VAR = "REDIS_DATABASE";
  public static final String REDIS_SSL_ENV_VAR = "REDIS_SSL";

  public static final String REDIS_ENTERPRISE_ENV_VAR = "REDIS_ENTERPRISE";
  public static final String REDIS_ENTERPRISE_USERNAME_ENV_VAR = "REDIS_ENTERPRISE_USERNAME";
  public static final String REDIS_ENTERPRISE_PASSWORD_ENV_VAR = "REDIS_ENTERPRISE_PASSWORD";

  public RedisConfig() {}

  public RedisConfig(Properties properties) {
    String redisHostEnvVar = System.getenv(REDIS_HOST_ENV_VAR);
    String redisPortEnvVar = System.getenv(REDIS_PORT_ENV_VAR);
    String redisUsernameEnvVar = System.getenv(REDIS_USERNAME_ENV_VAR);
    String redisPasswordEnvVar = System.getenv(REDIS_PASSWORD_ENV_VAR);
    String redisDatabaseEnvVar = System.getenv(REDIS_DATABASE_ENV_VAR);
    String redisSslEnvVar = System.getenv(REDIS_SSL_ENV_VAR);

    String redisEnterpriseEnvVar = System.getenv(REDIS_ENTERPRISE_ENV_VAR);
    String redisEnterpriseUserNameEnvVar = System.getenv(REDIS_ENTERPRISE_USERNAME_ENV_VAR);
    String redisEnterprisePasswordEnvVar = System.getenv(REDIS_ENTERPRISE_PASSWORD_ENV_VAR);

    this.redisHost = properties.getProperty(REDIS_HOST, "localhost");
    this.redisPassword = properties.getProperty(REDIS_PASSWORD);
    this.redisUsername = properties.getProperty(REDIS_USERNAME);
    this.redisPort = Integer.parseInt(properties.getProperty(REDIS_PORT, "6379"));
    this.redisDatabase = Integer.parseInt(properties.getProperty(REDIS_DATABASE, "0"));
    this.sslEnabled = Boolean.parseBoolean(properties.getProperty(REDIS_SSL, "false"));

    this.enterpriseDb = Boolean.parseBoolean(properties.getProperty(REDIS_ENTERPRISE, "false"));
    this.redisEnterpriseUserName = properties.getProperty(REDIS_ENTERPRISE_USERNAME);
    this.redisEnterprisePassword = properties.getProperty(REDIS_ENTERPRISE_PASSWORD);
    this.redisEnterpriseApiPort = Integer.parseInt(properties.getProperty(REDIS_ENTERPRISE_API_PORT, "9443"));
    this.redisEnterpriseShards = Integer.parseInt(properties.getProperty(REDIS_ENTERPRISE_SHARDS, "1"));
    this.redisEnterpriseMemory = Integer.parseInt(properties.getProperty(REDIS_ENTERPRISE_MEMORY, "1073741824"));

    if (redisHostEnvVar != null && !redisHostEnvVar.isEmpty()) {
      this.redisHost = redisHostEnvVar;
    }
    if (redisPortEnvVar != null && !redisPortEnvVar.isEmpty()) {
      this.redisPort = Integer.parseInt(redisPortEnvVar);
    }
    if (redisUsernameEnvVar != null && !redisUsernameEnvVar.isEmpty()) {
      this.redisUsername = redisUsernameEnvVar;
    }
    if (redisPasswordEnvVar != null && !redisPasswordEnvVar.isEmpty()) {
      this.redisPassword = redisPasswordEnvVar;
    }
    if (redisDatabaseEnvVar != null && !redisDatabaseEnvVar.isEmpty()) {
      this.redisDatabase = Integer.parseInt(redisDatabaseEnvVar);
    }
    if (redisSslEnvVar != null && !redisSslEnvVar.isEmpty()) {
      this.sslEnabled = Boolean.parseBoolean(redisSslEnvVar);
    }
    if (redisEnterpriseEnvVar != null && !redisEnterpriseEnvVar.isEmpty()) {
      this.enterpriseDb = Boolean.parseBoolean(redisEnterpriseEnvVar);
    }
    if (redisEnterpriseUserNameEnvVar != null && !redisEnterpriseUserNameEnvVar.isEmpty()) {
      this.redisEnterpriseUserName = redisEnterpriseUserNameEnvVar;
    }
    if (redisEnterprisePasswordEnvVar != null && !redisEnterprisePasswordEnvVar.isEmpty()) {
      this.redisEnterprisePassword = redisEnterprisePasswordEnvVar;
    }
  }

  public String getRedisHost() {
    return redisHost;
  }

  public String getRedisPassword() {
    return redisPassword;
  }

  public String getRedisUsername() {
    return redisUsername;
  }

  public int getRedisPort() {
    return redisPort;
  }

  public int getRedisDatabase() {
    return redisDatabase;
  }

  public boolean isSslEnabled() {
    return sslEnabled;
  }

  public boolean isEnterpriseDb() {
    return enterpriseDb;
  }

  public String getRedisEnterpriseUserName() {
    return redisEnterpriseUserName;
  }

  public String getRedisEnterprisePassword() {
    return redisEnterprisePassword;
  }

  public int getRedisEnterpriseApiPort() {
    return redisEnterpriseApiPort;
  }

  public int getRedisEnterpriseShards() {
    return redisEnterpriseShards;
  }

  public int getRedisEnterpriseMemory() {
    return redisEnterpriseMemory;
  }

  public RedisURI getRedisURI() {
    RedisURI redisURI = new RedisURI();
    redisURI.setHost(redisHost);
    redisURI.setDatabase(redisDatabase);
    redisURI.setPort(redisPort);

    if (redisPassword != null && !redisPassword.isEmpty()) {
      String userName;
      if (redisUsername != null && !redisUsername.isEmpty()) {
        userName = redisUsername;
      } else {
        userName = "";
      }
      StaticCredentialsProvider password = new StaticCredentialsProvider(userName, redisPassword.toCharArray());
      redisURI.setCredentialsProvider(password);
    }

    if (sslEnabled) {
      redisURI.setSsl(true);
      redisURI.setVerifyPeer(false);
    }
    return redisURI;
  }

  @Override
  public String toString() {
    return getRedisURI().toString();
  }
}
