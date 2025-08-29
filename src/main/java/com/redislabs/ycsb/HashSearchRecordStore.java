package com.redislabs.ycsb;

import com.codelry.util.ycsb.ByteIterator;
import com.codelry.util.ycsb.Status;
import com.codelry.util.ycsb.StringByteIterator;

import com.redis.lettucemod.RedisModulesClient;
import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import io.lettuce.core.RedisURI;
import io.lettuce.core.search.SearchReply;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import io.lettuce.core.support.ConnectionPoolSupport;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HashSearchRecordStore implements RecordStore {
  private static final Logger logger = LoggerFactory.getLogger(HashSearchRecordStore.class);

  private static final AtomicInteger THREADS = new AtomicInteger(0);
  private static final Object INIT_COORDINATOR = new Object();
  private static final GenericObjectPoolConfig<StatefulRedisModulesConnection<String, String>> poolConfig = new GenericObjectPoolConfig<>();
  private static GenericObjectPool<StatefulRedisModulesConnection<String, String>> pool;
  private static RedisModulesClient client;

  private final String indexName;

  HashSearchRecordStore(RedisConfig redisConfig) {
    synchronized (INIT_COORDINATOR) {
      THREADS.incrementAndGet();
      if (client == null) {
        logger.debug("Initializing Redis client: datatype: Hash, index: Search");
        poolConfig.setMaxTotal(8);
        poolConfig.setMaxIdle(4);
        poolConfig.setMinIdle(2);
        poolConfig.setTestOnBorrow(true);
        poolConfig.setTestOnReturn(true);
        poolConfig.setTestWhileIdle(true);
        poolConfig.setBlockWhenExhausted(true);

        RedisURI redisURI = redisConfig.getRedisURI();
        client = RedisModulesClient.create(redisURI);

        pool = ConnectionPoolSupport.createGenericObjectPool(
            client::connect,
            poolConfig
        );
      }
    }

    this.indexName = redisConfig.getIndexHash();
  }

  private String keyNumber(String key) {
    return key.substring(4);
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
    Map<String, String> map;
    try (StatefulRedisModulesConnection<String, String> connection = pool.borrowObject()) {
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
    map.put("id", keyNumber(key));

    try (StatefulRedisModulesConnection<String, String> connection = pool.borrowObject()) {
      result = connection.sync().hmset(key, map);
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
    map.put("id", keyNumber(key));

    try (StatefulRedisModulesConnection<String, String> connection = pool.borrowObject()) {
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

    try (StatefulRedisModulesConnection<String, String> connection = pool.borrowObject()) {
      result = connection.sync().del(key);
    } catch (Exception e) {
      logger.error("Error during Hash delete: {}", e.getMessage(), e);
      return Status.ERROR;
    }

    return result.equals(0L) ? Status.ERROR : Status.OK;
  }

  @Override
  public Status scan(String table, String key, int count, Set<String> fields,
                     Vector<HashMap<String, ByteIterator>> result) {
    try (StatefulRedisModulesConnection<String, String> connection = pool.borrowObject()) {
      String query = String.format("@id:[%s +inf] LIMIT 0 %d", keyNumber(key), count);

      CompletableFuture<SearchReply<String, String>> searchFuture =
          connection.async().ftSearch(indexName, query).toCompletableFuture();

      SearchReply<String, String> searchResult = searchFuture.join();

      List<CompletableFuture<HashMap<String, ByteIterator>>> futures = new ArrayList<>();

      for (SearchReply.SearchResult<String, String> r : searchResult.getResults()) {
        String docId = r.getId();

        CompletableFuture<HashMap<String, ByteIterator>> fetchFuture;
        if (fields == null) {
          fetchFuture =
              connection.async().hgetall(docId)
                  .toCompletableFuture()
                  .thenApply(map -> {
                    HashMap<String, ByteIterator> values = new HashMap<>(map.size());
                    StringByteIterator.putAllAsByteIterators(values, map);
                    return values;
                  });
        } else {
          fetchFuture =
              connection.async().hgetall(docId)
                  .toCompletableFuture()
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
                  });
        }

        futures.add(fetchFuture);
      }

      CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();

      for (CompletableFuture<HashMap<String, ByteIterator>> f : futures) {
        result.add(f.join());
      }

      return Status.OK;
    } catch (Exception e) {
      logger.error("Error during Hash search: {}", e.getMessage(), e);
      return Status.ERROR;
    }
  }
}
