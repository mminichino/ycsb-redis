package com.redislabs.ycsb;

import io.lettuce.core.*;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.api.StatefulRedisConnection;

import com.codelry.util.ycsb.ByteIterator;
import com.codelry.util.ycsb.DB;
import com.codelry.util.ycsb.DBException;
import com.codelry.util.ycsb.Status;
import com.codelry.util.ycsb.StringByteIterator;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.concurrent.CompletableFuture;

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
  private static final String INDEX_KEY = "_key_index";
  private static final Object INIT_COORDINATOR = new Object();

  private static RedisURI redisURI;
  private static boolean enterpriseDb;

  private StatefulRedisConnection<String, String> connection;
  private RedisCommands<String, String> syncCommands;
  private RedisAsyncCommands<String, String> asyncCommands;

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
      }
    }

    try {
      RedisClient redisClient = RedisClient.create(redisURI);
      connection = redisClient.connect();
      syncCommands = connection.sync();
      asyncCommands = connection.async();
    } catch (Exception e) {
      logger.error("Error connecting to Redis: {}", e.getMessage());
      throw new DBException(e);
    }
  }

  @Override
  public void cleanup() {
    connection.close();
  }

  private double hash(String key) {
    return key.hashCode();
  }

  @Override
  public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    if (fields == null) {
      StringByteIterator.putAllAsByteIterators(result, syncCommands.hgetall(key));
    } else {
      String[] fieldArray = fields.toArray(new String[0]);
      List<KeyValue<String, String>> values = syncCommands.hmget(key, fieldArray);

      for (KeyValue<String, String> entry : values) {
        result.put(entry.getKey(), new StringByteIterator(entry.getValue()));
      }
    }
    return result.isEmpty() ? Status.ERROR : Status.OK;
  }

  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    values.put("id", new StringByteIterator(key));
    if (syncCommands.hmset(key, StringByteIterator.getStringMap(values)).equals("OK")) {
      if (!enterpriseDb) addKeyToIndex(hash(key), key);
      return Status.OK;
    }
    return Status.ERROR;
  }

  @Override
  public Status delete(String table, String key) {
    if (syncCommands.del(key).equals(0L)) {
      return Status.ERROR;
    } else {
      if (!enterpriseDb) removeKeyFromIndex(key);
      return Status.OK;
    }
  }

  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {
    values.put("id", new StringByteIterator(key));
    return syncCommands.hmset(key, StringByteIterator.getStringMap(values))
        .equals("OK") ? Status.OK : Status.ERROR;
  }

  public void addKeyToIndex(double hash, String key) {
    syncCommands.zadd(INDEX_KEY, hash, key);
  }

  public void removeKeyFromIndex(String key) {
    syncCommands.zrem(INDEX_KEY, key);
  }

  public List<String> scanKeys(double hash, int recordCount) {
    return syncCommands.zrangebyscore(INDEX_KEY, Range.create(hash, Double.POSITIVE_INFINITY), Limit.from(recordCount));
  }

  @Override
  public Status scan(String table, String startkey, int recordcount,
                     Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    List<String> keys = scanKeys(hash(startkey), recordcount);

    if (keys.isEmpty()) {
      return Status.OK;
    }

    try {
      List<CompletableFuture<HashMap<String, ByteIterator>>> futures = new ArrayList<>(keys.size());

      for (String key : keys) {
        if (fields == null) {
          futures.add(
              asyncCommands.hgetall(key).toCompletableFuture()
                  .thenApply(map -> {
                    HashMap<String, ByteIterator> values = new HashMap<>(map.size());
                    StringByteIterator.putAllAsByteIterators(values, map);
                    return values;
                  })
          );
        } else {
          String[] fieldArray = fields.toArray(new String[0]);
          futures.add(
              asyncCommands.hmget(key, fieldArray).toCompletableFuture()
                  .thenApply(list -> {
                    HashMap<String, ByteIterator> values = new HashMap<>(list.size());
                    for (KeyValue<String, String> kv : list) {
                      values.put(kv.getKey(), new StringByteIterator(kv.getValue()));
                    }
                    return values;
                  })
          );
        }
      }

      CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();

      for (CompletableFuture<HashMap<String, ByteIterator>> f : futures) {
        result.add(f.join());
      }

      return Status.OK;
    } catch (Exception e) {
      logger.error("Error during scan: {}", e.getMessage(), e);
      return Status.ERROR;
    }
  }
}
