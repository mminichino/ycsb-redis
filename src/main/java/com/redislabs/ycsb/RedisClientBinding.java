package com.redislabs.ycsb;

import io.lettuce.core.*;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.api.StatefulRedisConnection;
import com.redis.lettucemod.RedisModulesClient;
import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.lettucemod.api.sync.RedisModulesCommands;
import com.redis.lettucemod.api.async.RedisModulesAsyncCommands;

import com.codelry.util.ycsb.ByteIterator;
import com.codelry.util.ycsb.DB;
import com.codelry.util.ycsb.DBException;
import com.codelry.util.ycsb.Status;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
  Redis client binding for YCSB.

  All YCSB records are mapped to a Redis hash datatype.  For scanning
  operations, all keys are saved (by an arbitrary hash) in a sorted set.
 */
public class RedisClientBinding extends DB {

  private static final Logger logger = LoggerFactory.getLogger(RedisClientBinding.class);

  private static final String PROPERTY_FILE = "db.properties";
  private static final Object INIT_COORDINATOR = new Object();

  private static RedisURI redisURI;
  private static boolean enterpriseDb;
  private static String searchStrategy;
  private static String indexHash;
  private static String indexJson;
  private static String indexSet;

  private RecordStore recordStore;

  public void init() throws DBException {
    ClassLoader classloader = RedisClientBinding.class.getClassLoader();
    Properties properties = new Properties();

    try (InputStream in = classloader.getResourceAsStream(PROPERTY_FILE)) {
      if (in != null) {
        logger.debug("Loading properties from resource {}", PROPERTY_FILE);
        properties.load(in);
      } else {
        logger.warn("Resource {} not found on classpath", PROPERTY_FILE);
      }
    } catch (IOException e) {
      throw new DBException(e);
    }

    properties.putAll(getProperties());

    synchronized (INIT_COORDINATOR) {
      if (redisURI == null) {
        RedisConfig redisConfig = new RedisConfig(properties);
        redisURI = redisConfig.getRedisURI();
        enterpriseDb = redisConfig.isEnterpriseDb();
        searchStrategy = redisConfig.getSearchStrategy();
        indexHash = redisConfig.getIndexHash();
        indexJson = redisConfig.getIndexJson();
        indexSet = redisConfig.getIndexSet();
      }
    }

    try {
      if (enterpriseDb) {
        RedisModulesClient modulesClient = RedisModulesClient.create(redisURI);
        StatefulRedisModulesConnection<String, String> modulesConnection = modulesClient.connect();
        RedisModulesCommands<String, String> modulesCommands = modulesConnection.sync();
        RedisModulesAsyncCommands<String, String> modulesAsyncCommands = modulesConnection.async();
        if (searchStrategy.equals("JSON")) {
          String indexName = indexJson;
          recordStore = new JsonRecordStore(modulesCommands, modulesAsyncCommands, indexName);
        } else {
          String indexName = indexHash;
          recordStore = new HashSearchRecordStore(modulesCommands, modulesAsyncCommands, indexName);
        }
      } else {
        RedisClient redisClient = RedisClient.create(redisURI);
        StatefulRedisConnection<String, String> connection = redisClient.connect();
        RedisCommands<String, String> syncCommands = connection.sync();
        RedisAsyncCommands<String, String> asyncCommands = connection.async();
        String indexName = indexSet;
        recordStore = new HashRecordStore(syncCommands, asyncCommands, indexName);
      }
    } catch (Exception e) {
      logger.error("Error connecting to Redis: {}", e.getMessage());
      throw new DBException(e);
    }
  }

  @Override
  public void cleanup() {
  }

  @Override
  public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    return recordStore.read(table, key, fields, result);
  }

  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    return recordStore.insert(table, key, values);
  }

  @Override
  public Status delete(String table, String key) {
    return recordStore.delete(table, key);
  }

  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {
    return recordStore.update(table, key, values);
  }

  @Override
  public Status scan(String table, String startkey, int recordcount,
                     Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    return recordStore.scan(table, startkey, recordcount, fields, result);
  }
}
