package com.redislabs.ycsb;

import com.codelry.util.ycsb.ByteIterator;
import com.codelry.util.ycsb.Status;
import com.codelry.util.ycsb.StringByteIterator;

import io.lettuce.core.Range;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import io.lettuce.core.support.ConnectionPoolSupport;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HashRecordStore implements RecordStore {
  private static final Logger logger = LoggerFactory.getLogger(HashRecordStore.class);

  private static final AtomicInteger THREADS = new AtomicInteger(0);
  private static final AtomicLong COUNTER = new AtomicLong(0);
  private static final Object INIT_COORDINATOR = new Object();
  private static final GenericObjectPoolConfig<StatefulRedisConnection<String, String>> poolConfig = new GenericObjectPoolConfig<>();
  private static GenericObjectPool<StatefulRedisConnection<String, String>> pool;
  private static RedisClient client;

  private final String indexName;

  HashRecordStore(RedisConfig redisConfig) {
    synchronized (INIT_COORDINATOR) {
      THREADS.incrementAndGet();
      if (client == null) {
        poolConfig.setMaxTotal(32);
        poolConfig.setMaxIdle(10);
        poolConfig.setMinIdle(2);
        poolConfig.setTestOnBorrow(true);
        poolConfig.setTestOnReturn(true);
        poolConfig.setTestWhileIdle(true);
        poolConfig.setBlockWhenExhausted(true);

        RedisURI redisURI = redisConfig.getRedisURI();
        client = RedisClient.create(redisURI);

        pool = ConnectionPoolSupport.createGenericObjectPool(
            client::connect,
            poolConfig
        );
      }
    }

    this.indexName = redisConfig.getIndexSet();
  }

  public void addKeyToIndex(StatefulRedisConnection<String, String> connection, String key) {
    connection.sync().zadd(indexName, COUNTER.incrementAndGet(), key);
  }

  public void removeKeyFromIndex(StatefulRedisConnection<String, String> connection, String key) {
    connection.sync().zrem(indexName, key);
  }

  public List<String> scanKeys(StatefulRedisConnection<String, String> connection, double id, int count) {
    return connection.sync().zrangebyscore(indexName, Range.create(id, id + count - 1));
  }

  @Override
  public void disconnect() {
    synchronized (INIT_COORDINATOR) {
      int count = THREADS.decrementAndGet();
      if (client != null && count == 0) {
        pool.close();
        client.shutdown();
        pool = null;
        client = null;
      }
    }
  }

  @Override
  public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    Map<String, String> map;
    try (StatefulRedisConnection<String, String> connection = pool.borrowObject()) {
      map = connection.sync().hgetall(key);
    } catch (Exception e) {
      logger.error("Error during Hash read: {}", e.getMessage(), e);
      return Status.ERROR;
    }

    if (fields == null) {
      StringByteIterator.putAllAsByteIterators(result, map);
    } else {
      for (Map.Entry<String, String> entry : map.entrySet()) {
        if (fields.contains(entry.getKey())) {
          result.put(entry.getKey(), new StringByteIterator(entry.getValue()));
        }
      }
    }
    return result.isEmpty() ? Status.ERROR : Status.OK;
  }

  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    String result;
    Map<String, String> map = StringByteIterator.getStringMap(values);

    try (StatefulRedisConnection<String, String> connection = pool.borrowObject()) {
      result = connection.sync().hmset(key, map);
      addKeyToIndex(connection, key);
    } catch (Exception e) {
      logger.error("Error during Hash insert: {}", e.getMessage(), e);
      return Status.ERROR;
    }

    return result.equals("OK") ? Status.OK : Status.ERROR;
  }

  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {
    String result;
    Map<String, String> map = StringByteIterator.getStringMap(values);

    try (StatefulRedisConnection<String, String> connection = pool.borrowObject()) {
      result = connection.sync().hmset(key, map);
    } catch (Exception e) {
      logger.error("Error during Hash update: {}", e.getMessage(), e);
      return Status.ERROR;
    }

    return result.equals("OK") ? Status.OK : Status.ERROR;
  }

  @Override
  public Status delete(String table, String key) {
    Long result;

    try (StatefulRedisConnection<String, String> connection = pool.borrowObject()) {
      result = connection.sync().del(key);
      removeKeyFromIndex(connection, key);
    } catch (Exception e) {
      logger.error("Error during Hash delete: {}", e.getMessage(), e);
      return Status.ERROR;
    }

    return result.equals(0L) ? Status.ERROR : Status.OK;
  }

  @Override
  public Status scan(String table, String key, int count, Set<String> fields,
                     Vector<HashMap<String, ByteIterator>> result) {
    try (StatefulRedisConnection<String, String> connection = pool.borrowObject()) {
      double id = connection.sync().zscore(indexName, key);
      List<String> keys = scanKeys(connection, id, count);

      if (keys.isEmpty()) {
        return Status.OK;
      }

      List<CompletableFuture<HashMap<String, ByteIterator>>> futures = new ArrayList<>(keys.size());

      for (String k : keys) {
        if (fields == null) {
          futures.add(
              connection.async().hgetall(k).toCompletableFuture()
                  .thenApply(map -> {
                    HashMap<String, ByteIterator> values = new HashMap<>(map.size());
                    StringByteIterator.putAllAsByteIterators(values, map);
                    return values;
                  })
          );
        } else {
          futures.add(
              connection.async().hgetall(k).toCompletableFuture()
                  .thenApply(map -> {
                    Map<String, String> subset = map.entrySet().stream()
                        .filter(entry -> fields.contains(entry.getKey()))
                        .collect(Collectors.toMap(
                            Map.Entry::getKey,
                            Map.Entry::getValue
                        ));
                    HashMap<String, ByteIterator> values = new HashMap<>();
                    StringByteIterator.putAllAsByteIterators(values, subset);
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
      logger.error("Error during Hash scan: {}", e.getMessage(), e);
      return Status.ERROR;
    }
  }
}
