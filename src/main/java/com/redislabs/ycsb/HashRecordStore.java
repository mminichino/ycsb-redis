package com.redislabs.ycsb;

import com.codelry.util.ycsb.ByteIterator;
import com.codelry.util.ycsb.Status;
import com.codelry.util.ycsb.StringByteIterator;

import io.lettuce.core.Limit;
import io.lettuce.core.Range;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.api.sync.RedisCommands;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HashRecordStore implements RecordStore {
  private static final Logger logger = LoggerFactory.getLogger(HashRecordStore.class);

  private static final AtomicInteger THREADS = new AtomicInteger(0);
  private static final Object INIT_COORDINATOR = new Object();
  private static GenericObjectPool<StatefulRedisConnection<String, String>> pool;
  private static RedisClient client;

  private final String indexName;

  HashRecordStore(RedisConfig redisConfig, int poolMaxSize) {
    synchronized (INIT_COORDINATOR) {
      THREADS.incrementAndGet();
      if (client == null) {
        logger.info("Initializing Redis client: datatype: Hash, index: Sorted Set");

        RedisClientBuilder clientBuilder = new RedisClientBuilder.Builder().redisConfig(redisConfig).build();
        client = clientBuilder.getClient();
        pool = clientBuilder.getPool(client, Math.max(poolMaxSize, 8));
      }
    }

    this.indexName = redisConfig.getIndexSet();
  }

  private static double hash(String key) {
    return key.hashCode();
  }

  @Override
  public void disconnect() {
    synchronized (INIT_COORDINATOR) {
      int count = THREADS.decrementAndGet();
      if (client != null && count == 0) {
        logger.debug("Shutting down Redis client");
        pool.close();
        client.shutdown();
        pool = null;
        client = null;
      }
    }
  }

  @Override
  public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    final Map<String, String> hash;
    try (StatefulRedisConnection<String, String> connection = pool.borrowObject()) {
      hash = connection.sync().hgetall(key);
    } catch (Exception e) {
      logger.error("Error during Hash read: {}", e.getMessage(), e);
      return Status.ERROR;
    }

    Map<String, ByteIterator> map = StringByteIterator.getByteIteratorMap(hash);
    if (fields == null) {
      result.putAll(map);
    } else {
      for (String field : fields) {
        ByteIterator value = map.get(field);
        if (value != null) {
          result.put(field, value);
        }
      }
    }
    return result.isEmpty() ? Status.ERROR : Status.OK;
  }

  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    Map<String, String> map = StringByteIterator.getStringMap(values);
    String result;

    try (StatefulRedisConnection<String, String> connection = pool.borrowObject()) {
      RedisCommands<String, String> sync = connection.sync();
      result = sync.hmset(key, map);
      sync.zadd(indexName, hash(key), key);
    } catch (Exception e) {
      logger.error("Error during Hash insert: {}", e.getMessage(), e);
      return Status.ERROR;
    }

    return result.equals("OK") ? Status.OK : Status.ERROR;
  }

  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {
    Map<String, String> map = StringByteIterator.getStringMap(values);
    String result;

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
    Long deleted;
    Long removed;

    try (StatefulRedisConnection<String, String> connection = pool.borrowObject()) {
      RedisCommands<String, String> sync = connection.sync();
      deleted = sync.del(key);
      removed = sync.zrem(indexName, key);
    } catch (Exception e) {
      logger.error("Error during Hash delete: {}", e.getMessage(), e);
      return Status.ERROR;
    }

    return deleted == 0L && removed == 0L ? Status.ERROR : Status.OK;
  }

  @Override
  public Status scan(String table, String startkey, int count, Set<String> fields,
                     Vector<HashMap<String, ByteIterator>> result) {
    try (StatefulRedisConnection<String, String> connection = pool.borrowObject()) {
      Range<Double> range = Range.from(
          Range.Boundary.including(hash(startkey)),
          Range.Boundary.unbounded());

      List<String> keys = connection.reactive()
          .zrangebyscore(indexName, range, Limit.create(0, count))
          .collectList()
          .block();

      if (keys == null || keys.isEmpty()) {
        return Status.OK;
      }

      RedisAsyncCommands<String, String> async = connection.async();
      List<CompletableFuture<HashMap<String, ByteIterator>>> futures = new ArrayList<>(keys.size());

      for (String key : keys) {
        futures.add(async.hgetall(key).toCompletableFuture().thenApply(hash -> {
          Map<String, ByteIterator> map = StringByteIterator.getByteIteratorMap(hash);
          HashMap<String, ByteIterator> values = new HashMap<>();
          if (fields == null) {
            values.putAll(map);
          } else {
            for (String field : fields) {
              ByteIterator value = map.get(field);
              if (value != null) {
                values.put(field, value);
              }
            }
          }
          return values;
        }));
      }

      CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
      for (CompletableFuture<HashMap<String, ByteIterator>> future : futures) {
        result.add(future.join());
      }
      return Status.OK;
    } catch (Exception e) {
      logger.error("Error during Hash scan: {}", e.getMessage(), e);
      return Status.ERROR;
    }
  }
}
