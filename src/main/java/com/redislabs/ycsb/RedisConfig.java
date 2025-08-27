package com.redislabs.ycsb;

import io.lettuce.core.RedisURI;
import io.lettuce.core.StaticCredentialsProvider;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RedisConfig {
  public static final Logger logger = LoggerFactory.getLogger(RedisConfig.class);

  private String redisHost;
  private String redisPassword;
  private String redisUsername;
  private int redisPort;
  private int redisDatabase;
  private boolean sslEnabled;
  private String dataPersistence;

  private String searchStrategy;
  private String indexHash;
  private String indexJson;
  private String indexSet;

  private boolean enterpriseDb;
  private int redisEnterpriseDbUid;
  private String redisEnterpriseUserName;
  private String redisEnterprisePassword;
  private String redisEnterpriseApiHost;
  private int redisEnterpriseApiPort;
  private int redisEnterpriseShards;
  private String redisEnterpriseShardPlacement;
  private long redisEnterpriseMemory;
  private boolean redisEnterpriseReplication;

  private String confirmationResponse;

  public static final String REDIS_HOST = "redis.host";
  public static final String REDIS_PORT = "redis.port";
  public static final String REDIS_USERNAME = "redis.username";
  public static final String REDIS_PASSWORD = "redis.password";
  public static final String REDIS_DATABASE = "redis.database";
  public static final String REDIS_SSL = "redis.ssl";
  public static final String REDIS_DATA_PERSISTENCE = "redis.data.persistence";

  public static final String REDIS_SEARCH_STRATEGY = "redis.search.strategy";
  public static final String REDIS_INDEX_HASH = "redis.index.hash";
  public static final String REDIS_INDEX_JSON = "redis.index.json";
  public static final String REDIS_INDEX_SET = "redis.index.set";

  public static final String REDIS_ENTERPRISE = "redis.enterprise";
  public static final String REDIS_ENTERPRISE_DB_UID = "redis.enterprise.db.uid";
  public static final String REDIS_ENTERPRISE_USERNAME = "redis.enterprise.username";
  public static final String REDIS_ENTERPRISE_PASSWORD = "redis.enterprise.password";
  public static final String REDIS_ENTERPRISE_API_HOST = "redis.enterprise.api.host";
  public static final String REDIS_ENTERPRISE_API_PORT = "redis.enterprise.api.port";
  public static final String REDIS_ENTERPRISE_SHARDS = "redis.enterprise.shards";
  public static final String REDIS_ENTERPRISE_SHARD_PLACEMENT = "redis.enterprise.shard.placement";
  public static final String REDIS_ENTERPRISE_MEMORY = "redis.enterprise.memory";
  public static final String REDIS_ENTERPRISE_REPLICATION = "redis.enterprise.replication";

  public static final String REDIS_HOST_ENV_VAR = "REDIS_HOST";
  public static final String REDIS_PORT_ENV_VAR = "REDIS_PORT";
  public static final String REDIS_USERNAME_ENV_VAR = "REDIS_USERNAME";
  public static final String REDIS_PASSWORD_ENV_VAR = "REDIS_PASSWORD";
  public static final String REDIS_DATABASE_ENV_VAR = "REDIS_DATABASE";
  public static final String REDIS_SSL_ENV_VAR = "REDIS_SSL";

  public static final String REDIS_ENTERPRISE_ENV_VAR = "REDIS_ENTERPRISE";
  public static final String REDIS_ENTERPRISE_USERNAME_ENV_VAR = "REDIS_ENTERPRISE_USERNAME";
  public static final String REDIS_ENTERPRISE_PASSWORD_ENV_VAR = "REDIS_ENTERPRISE_PASSWORD";
  public static final String REDIS_ENTERPRISE_API_HOST_ENV_VAR = "REDIS_ENTERPRISE_API_HOST";

  public static final String TEST_CONFIRMATION_RESPONSE = "test.confirmation.response";

  public static final String PROPERTY_FILE = "db.properties";

  public RedisConfig() {}

  public RedisConfig(Properties properties) {
    properties.putAll(getProperties());

    String redisHostEnvVar = System.getenv(REDIS_HOST_ENV_VAR);
    String redisPortEnvVar = System.getenv(REDIS_PORT_ENV_VAR);
    String redisUsernameEnvVar = System.getenv(REDIS_USERNAME_ENV_VAR);
    String redisPasswordEnvVar = System.getenv(REDIS_PASSWORD_ENV_VAR);
    String redisDatabaseEnvVar = System.getenv(REDIS_DATABASE_ENV_VAR);
    String redisSslEnvVar = System.getenv(REDIS_SSL_ENV_VAR);

    String redisEnterpriseEnvVar = System.getenv(REDIS_ENTERPRISE_ENV_VAR);
    String redisEnterpriseUserNameEnvVar = System.getenv(REDIS_ENTERPRISE_USERNAME_ENV_VAR);
    String redisEnterprisePasswordEnvVar = System.getenv(REDIS_ENTERPRISE_PASSWORD_ENV_VAR);
    String redisEnterpriseApiHostEnvVar = System.getenv(REDIS_ENTERPRISE_API_HOST_ENV_VAR);

    this.redisHost = properties.getProperty(REDIS_HOST, "localhost");
    this.redisPassword = properties.getProperty(REDIS_PASSWORD);
    this.redisUsername = properties.getProperty(REDIS_USERNAME);
    this.redisPort = Integer.parseInt(properties.getProperty(REDIS_PORT, "6379"));
    this.redisDatabase = Integer.parseInt(properties.getProperty(REDIS_DATABASE, "0"));
    this.sslEnabled = Boolean.parseBoolean(properties.getProperty(REDIS_SSL, "false"));
    this.dataPersistence = properties.getProperty(REDIS_DATA_PERSISTENCE, "AOF");

    this.searchStrategy = properties.getProperty(REDIS_SEARCH_STRATEGY, "HASH");
    this.indexHash = properties.getProperty(REDIS_INDEX_HASH, "id_hash_index");
    this.indexJson = properties.getProperty(REDIS_INDEX_JSON, "id_json_index");
    this.indexSet = properties.getProperty(REDIS_INDEX_SET, "_key_index");

    this.enterpriseDb = Boolean.parseBoolean(properties.getProperty(REDIS_ENTERPRISE, "false"));
    this.redisEnterpriseDbUid = Integer.parseInt(properties.getProperty(REDIS_ENTERPRISE_DB_UID, "1"));
    this.redisEnterpriseUserName = properties.getProperty(REDIS_ENTERPRISE_USERNAME);
    this.redisEnterprisePassword = properties.getProperty(REDIS_ENTERPRISE_PASSWORD);
    this.redisEnterpriseApiHost = properties.getProperty(REDIS_ENTERPRISE_API_HOST, "localhost");
    this.redisEnterpriseApiPort = Integer.parseInt(properties.getProperty(REDIS_ENTERPRISE_API_PORT, "9443"));
    this.redisEnterpriseShards = Integer.parseInt(properties.getProperty(REDIS_ENTERPRISE_SHARDS, "1"));
    this.redisEnterpriseShardPlacement = properties.getProperty(REDIS_ENTERPRISE_SHARD_PLACEMENT, "SPARSE");
    this.redisEnterpriseMemory = Long.parseLong(properties.getProperty(REDIS_ENTERPRISE_MEMORY, "1073741824"));
    this.redisEnterpriseReplication =  Boolean.parseBoolean(properties.getProperty(REDIS_ENTERPRISE_REPLICATION, "false"));

    this.confirmationResponse = properties.getProperty(TEST_CONFIRMATION_RESPONSE);

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
    if (redisEnterpriseApiHostEnvVar != null && !redisEnterpriseApiHostEnvVar.isEmpty()) {
      this.redisEnterpriseApiHost = redisEnterpriseApiHostEnvVar;
    }
  }

  public Properties getProperties() {
    ClassLoader classloader = Thread.currentThread().getContextClassLoader();
    if (classloader == null) {
      classloader = RedisConfig.class.getClassLoader();
    }
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

    return properties;
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

  public String getDataPersistence() {
    return dataPersistence;
  }

  public boolean isEnterpriseDb() {
    return enterpriseDb;
  }

  public String getSearchStrategy() {
    return searchStrategy;
  }

  public String getIndexHash() {
    return indexHash;
  }

  public String getIndexJson() {
    return indexJson;
  }

  public String getIndexSet() {
    return indexSet;
  }

  public int getRedisEnterpriseDbUid() {
    return redisEnterpriseDbUid;
  }

  public String getRedisEnterpriseUserName() {
    return redisEnterpriseUserName;
  }

  public String getRedisEnterprisePassword() {
    return redisEnterprisePassword;
  }

  public String getRedisEnterpriseApiHost() {
    return redisEnterpriseApiHost;
  }

  public int getRedisEnterpriseApiPort() {
    return redisEnterpriseApiPort;
  }

  public int getRedisEnterpriseShards() {
    return redisEnterpriseShards;
  }

  public String getRedisEnterpriseShardPlacement() {
      return redisEnterpriseShardPlacement;
  }

  public long getRedisEnterpriseMemory() {
    return redisEnterpriseMemory;
  }

  public boolean getRedisEnterpriseReplication() {
      return redisEnterpriseReplication;
  }

  public String getConfirmationResponse() {
    return confirmationResponse != null ? confirmationResponse : "no";
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
