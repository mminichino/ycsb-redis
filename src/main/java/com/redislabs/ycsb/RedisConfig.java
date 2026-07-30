package com.redislabs.ycsb;

import io.lettuce.core.RedisURI;
import io.lettuce.core.StaticCredentialsProvider;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;

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
  private boolean ossClusterEnabled;
  private String dataPersistence;
  private String indexSet;
  private List<String> clusterHosts = Collections.emptyList();

  private String confirmationResponse;

  public static final String REDIS_HOST = "redis.host";
  public static final String REDIS_PORT = "redis.port";
  public static final String REDIS_USERNAME = "redis.username";
  public static final String REDIS_PASSWORD = "redis.password";
  public static final String REDIS_DATABASE = "redis.database";
  public static final String REDIS_SSL = "redis.ssl";
  public static final String REDIS_OSS_CLUSTER = "redis.oss.cluster";
  public static final String REDIS_DATA_PERSISTENCE = "redis.data.persistence";
  public static final String REDIS_INDEX_SET = "redis.index.set";
  public static final String REDIS_CLUSTER_HOST_PREFIX = "redis.cluster.host.";

  public static final String REDIS_HOST_ENV_VAR = "REDIS_HOST";
  public static final String REDIS_PORT_ENV_VAR = "REDIS_PORT";
  public static final String REDIS_USERNAME_ENV_VAR = "REDIS_USERNAME";
  public static final String REDIS_PASSWORD_ENV_VAR = "REDIS_PASSWORD";
  public static final String REDIS_DATABASE_ENV_VAR = "REDIS_DATABASE";
  public static final String REDIS_SSL_ENV_VAR = "REDIS_SSL";
  public static final String REDIS_OSS_CLUSTER_ENV_VAR = "REDIS_OSS_CLUSTER";

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
    String redisOssClusterEnvVar = System.getenv(REDIS_OSS_CLUSTER_ENV_VAR);

    this.redisHost = properties.getProperty(REDIS_HOST, "localhost");
    this.redisPassword = properties.getProperty(REDIS_PASSWORD);
    this.redisUsername = properties.getProperty(REDIS_USERNAME, "default");
    this.redisPort = Integer.parseInt(properties.getProperty(REDIS_PORT, "6379"));
    this.redisDatabase = Integer.parseInt(properties.getProperty(REDIS_DATABASE, "0"));
    this.sslEnabled = Boolean.parseBoolean(properties.getProperty(REDIS_SSL, "false"));
    this.ossClusterEnabled = Boolean.parseBoolean(properties.getProperty(REDIS_OSS_CLUSTER, "false"));
    this.dataPersistence = properties.getProperty(REDIS_DATA_PERSISTENCE, "AOF");
    this.indexSet = properties.getProperty(REDIS_INDEX_SET, "_key_index");
    this.confirmationResponse = properties.getProperty(TEST_CONFIRMATION_RESPONSE);
    this.clusterHosts = parseClusterHosts(properties);

    if (!this.clusterHosts.isEmpty()) {
      this.ossClusterEnabled = true;
    }

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
    if (redisOssClusterEnvVar != null && !redisOssClusterEnvVar.isEmpty()) {
      this.ossClusterEnabled = Boolean.parseBoolean(redisOssClusterEnvVar);
    }

    String redisHostSysProp = System.getProperty(REDIS_HOST);
    String redisPortSysProp = System.getProperty(REDIS_PORT);
    if (redisHostSysProp != null && !redisHostSysProp.isEmpty()) {
      this.redisHost = redisHostSysProp;
    }
    if (redisPortSysProp != null && !redisPortSysProp.isEmpty()) {
      this.redisPort = Integer.parseInt(redisPortSysProp);
    }
  }

  private static List<String> parseClusterHosts(Properties properties) {
    Map<Integer, String> hostsByIndex = new TreeMap<>();
    for (String name : properties.stringPropertyNames()) {
      if (!name.startsWith(REDIS_CLUSTER_HOST_PREFIX)) {
        continue;
      }
      try {
        int index = Integer.parseInt(name.substring(REDIS_CLUSTER_HOST_PREFIX.length()));
        String value = properties.getProperty(name);
        if (value != null && !value.trim().isEmpty()) {
          hostsByIndex.put(index, value.trim());
        }
      } catch (NumberFormatException ignored) {
        // Ignore malformed cluster host property keys.
      }
    }
    return new ArrayList<>(hostsByIndex.values());
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

  public boolean isOssClusterEnabled() {
    return ossClusterEnabled || !clusterHosts.isEmpty();
  }

  public String getDataPersistence() {
    return dataPersistence;
  }

  public String getIndexSet() {
    return indexSet;
  }

  public List<String> getClusterHosts() {
    return Collections.unmodifiableList(clusterHosts);
  }

  public String getConfirmationResponse() {
    return confirmationResponse != null ? confirmationResponse : "no";
  }

  public RedisURI getRedisURI() {
    return buildRedisURI(redisHost, redisPort);
  }

  public List<RedisURI> getClusterRedisURIs() {
    List<RedisURI> uris = new ArrayList<>();
    if (clusterHosts.isEmpty()) {
      uris.add(getRedisURI());
      return uris;
    }

    for (String hostEntry : clusterHosts) {
      String host = hostEntry;
      int port = redisPort;
      int colon = hostEntry.lastIndexOf(':');
      if (colon > 0 && colon < hostEntry.length() - 1) {
        host = hostEntry.substring(0, colon);
        port = Integer.parseInt(hostEntry.substring(colon + 1));
      }
      uris.add(buildRedisURI(host, port));
    }
    return uris;
  }

  private RedisURI buildRedisURI(String host, int port) {
    RedisURI redisURI = new RedisURI();
    redisURI.setHost(host);
    redisURI.setDatabase(redisDatabase);
    redisURI.setPort(port);

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
    if (isOssClusterEnabled()) {
      return getClusterRedisURIs().toString();
    }
    return getRedisURI().toString();
  }
}
