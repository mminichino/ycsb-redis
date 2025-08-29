package com.redislabs.ycsb;

import io.lettuce.core.*;

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
  public static final String THREAD_COUNT_PROPERTY = "threadcount";

  private RecordStore recordStore;

  public void init() throws DBException {
    ClassLoader classloader = Thread.currentThread().getContextClassLoader();
    if (classloader == null) {
      classloader = RedisClientBinding.class.getClassLoader();
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
      throw new DBException(e);
    }

    properties.putAll(getProperties());

    RedisConfig redisConfig = new RedisConfig(properties);
    boolean enterpriseDb = redisConfig.isEnterpriseDb();
    String searchStrategy = redisConfig.getSearchStrategy();
    int threadCount = Integer.parseInt(properties.getProperty(THREAD_COUNT_PROPERTY, "32"));

    try {
      if (enterpriseDb) {
        if (searchStrategy.equals("JSON")) {
          recordStore = new JsonRecordStore(redisConfig, threadCount);
        } else {
          recordStore = new HashSearchRecordStore(redisConfig, threadCount);
        }
      } else {
        recordStore = new HashRecordStore(redisConfig, threadCount);
      }
    } catch (Exception e) {
      logger.error("Error connecting to Redis: {}", e.getMessage());
      throw new DBException(e);
    }
  }

  @Override
  public void cleanup() {
    recordStore.disconnect();
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
