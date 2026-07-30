package com.redislabs.ycsb;

import com.codelry.util.ycsb.ByteIterator;
import com.codelry.util.ycsb.Status;
import com.codelry.util.ycsb.StringByteIterator;

import io.lettuce.core.Limit;
import io.lettuce.core.Range;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.api.async.RedisAdvancedClusterAsyncCommands;
import io.lettuce.core.cluster.api.sync.RedisAdvancedClusterCommands;

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

public class HashClusterRecordStore implements RecordStore {
  private static final Logger logger = LoggerFactory.getLogger(HashClusterRecordStore.class);

  private static final AtomicInteger THREADS = new AtomicInteger(0);
  private static final Object INIT_COORDINATOR = new Object();
  private static GenericObjectPool<StatefulRedisClusterConnection<String, String>> pool;
  private static RedisClusterClient client;

  private final String indexName;

  HashClusterRecordStore(RedisConfig redisConfig, int poolMaxSize) {
    synchronized (INIT_COORDINATOR) {
      THREADS.incrementAndGet();
      if (client == null) {
        logger.info("Initializing Redis Cluster client: datatype: Hash, index: Sorted Set");

        RedisClientBuilder clientBuilder = new RedisClientBuilder.Builder().redisConfig(redisConfig).build();
        client = clientBuilder.getClusterClient();
        pool = clientBuilder.getClusterPool(client, Math.max(poolMaxSize, 8));
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
        logger.debug("Shutting down Redis Cluster client");
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
    try (StatefulRedisClusterConnection<String, String> connection = pool.borrowObject()) {
      hash = connection.sync().hgetall(key);
    } catch (Exception e) {
      logger.error("Error during Hash Cluster read: {}", e.getMessage(), e);
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

    try (StatefulRedisClusterConnection<String, String> connection = pool.borrowObject()) {
      RedisAdvancedClusterCommands<String, String> sync = connection.sync();
      result = sync.hmset(key, map);
      sync.zadd(indexName, hash(key), key);
    } catch (Exception e) {
      logger.error("Error during Hash Cluster insert: {}", e.getMessage(), e);
      return Status.ERROR;
    }

    return result.equals("OK") ? Status.OK : Status.ERROR;
  }

  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {
    Map<String, String> map = StringByteIterator.getStringMap(values);
    String result;

    try (StatefulRedisClusterConnection<String, String> connection = pool.borrowObject()) {
      result = connection.sync().hmset(key, map);
    } catch (Exception e) {
      logger.error("Error during Hash Cluster update: {}", e.getMessage(), e);
      return Status.ERROR;
    }

    return result.equals("OK") ? Status.OK : Status.ERROR;
  }

  @Override
  public Status delete(String table, String key) {
    Long deleted;
    Long removed;

    try (StatefulRedisClusterConnection<String, String> connection = pool.borrowObject()) {
      RedisAdvancedClusterCommands<String, String> sync = connection.sync();
      deleted = sync.del(key);
      removed = sync.zrem(indexName, key);
    } catch (Exception e) {
      logger.error("Error during Hash Cluster delete: {}", e.getMessage(), e);
      return Status.ERROR;
    }

    return deleted == 0L && removed == 0L ? Status.ERROR : Status.OK;
  }

  @Override
  public Status scan(String table, String startkey, int count, Set<String> fields,
                     Vector<HashMap<String, ByteIterator>> result) {
    try (StatefulRedisClusterConnection<String, String> connection = pool.borrowObject()) {
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

      RedisAdvancedClusterAsyncCommands<String, String> async = connection.async();
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
      logger.error("Error during Hash Cluster scan: {}", e.getMessage(), e);
      return Status.ERROR;
    }
  }
}
