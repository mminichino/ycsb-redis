package com.redislabs.ycsb;

import com.codelry.util.ycsb.ByteIterator;
import com.codelry.util.ycsb.Status;

import com.codelry.util.ycsb.StringByteIterator;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;

import com.redis.lettucemod.RedisModulesClient;
import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import io.lettuce.core.RedisURI;
import io.lettuce.core.json.JsonPath;
import io.lettuce.core.search.SearchReply;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import io.lettuce.core.support.ConnectionPoolSupport;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JsonRecordStore implements RecordStore {
  private static final Logger logger = LoggerFactory.getLogger(JsonRecordStore.class);

  private static final AtomicInteger THREADS = new AtomicInteger(0);
  private static final Object INIT_COORDINATOR = new Object();
  private static final GenericObjectPoolConfig<StatefulRedisModulesConnection<String, String>> poolConfig = new GenericObjectPoolConfig<>();
  private static GenericObjectPool<StatefulRedisModulesConnection<String, String>> pool;
  private static RedisModulesClient client;

  private final String indexName;
  private final ObjectMapper mapper = new ObjectMapper();
  private final TypeReference<Map<String, ByteIterator>> typeRef = new TypeReference<Map<String, ByteIterator>>() {};

  JsonRecordStore(RedisConfig redisConfig) {
    synchronized (INIT_COORDINATOR) {
      THREADS.incrementAndGet();
      if (client == null) {
        logger.debug("Initializing Redis client: datatype: JSON, index: Search");
        poolConfig.setMaxTotal(20);
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

    this.indexName = redisConfig.getIndexJson();
    SimpleModule serializer = new SimpleModule("ByteIteratorSerializer");
    SimpleModule deserializer = new SimpleModule("ByteIteratorDeserializer");
    serializer.addSerializer(ByteIterator.class, new ByteIteratorSerializer());
    deserializer.addDeserializer(ByteIterator.class, new ByteIteratorDeserializer());
    this.mapper.registerModule(serializer);
    this.mapper.registerModule(deserializer);
  }

  private long hashKey(String key) {
    return Integer.toUnsignedLong(key.hashCode());
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
    Map<String, ByteIterator> map;

    try (StatefulRedisModulesConnection<String, String> connection = pool.borrowObject()) {
      String value = connection.sync().jsonGet(key, JsonPath.of("$")).get(0).asJsonArray().getFirst().toString();
      map = mapper.readValue(value, typeRef);
    } catch (Exception e) {
      logger.error("Error during JSON read: {}", e.getMessage(), e);
      return Status.ERROR;
    }

    if (fields == null) {
      result = map;
    } else {
      for (Map.Entry<String, ByteIterator> entry : map.entrySet()) {
        if (fields.contains(entry.getKey())) {
          result.put(entry.getKey(), entry.getValue());
        }
      }
    }

    return result.isEmpty() ? Status.ERROR : Status.OK;
  }

  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    String result;
    Map<String, Object> map = new HashMap<>(StringByteIterator.getStringMap(values));
    map.put("id", hashKey(key));

    try (StatefulRedisModulesConnection<String, String> connection = pool.borrowObject()) {
      result = connection.sync().jsonSet(key, JsonPath.of("$"), connection.sync().getJsonParser().fromObject(map));
    } catch (Exception e) {
      logger.error("Error during JSON insert: {}", e.getMessage(), e);
      return Status.ERROR;
    }

    return result.equals("OK") ? Status.OK : Status.ERROR;
  }

  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {
    String result;
    Map<String, Object> map = new HashMap<>(StringByteIterator.getStringMap(values));
    map.put("id", hashKey(key));

    try (StatefulRedisModulesConnection<String, String> connection = pool.borrowObject()) {
      result = connection.sync().jsonSet(key, JsonPath.of("$"), connection.sync().getJsonParser().fromObject(map));
    } catch (Exception e) {
      logger.error("Error during JSON update: {}", e.getMessage(), e);
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
      logger.error("Error during JSON delete: {}", e.getMessage(), e);
      return Status.ERROR;
    }

    return result.equals(0L) ? Status.ERROR : Status.OK;
  }

  @Override
  public Status scan(String table, String key, int count, Set<String> fields,
                     Vector<HashMap<String, ByteIterator>> result) {
    try (StatefulRedisModulesConnection<String, String> connection = pool.borrowObject()) {
      String query = String.format("@id:[%d +inf] LIMIT 0 %d", key.hashCode(), count);

      CompletableFuture<SearchReply<String, String>> searchFuture =
          connection.async().ftSearch(indexName, query).toCompletableFuture();

      SearchReply<String, String> searchResult = searchFuture.join();

      List<CompletableFuture<HashMap<String, ByteIterator>>> futures = new ArrayList<>();

      for (SearchReply.SearchResult<String, String> r : searchResult.getResults()) {
        String docId = r.getId();

        CompletableFuture<HashMap<String, ByteIterator>> fetchFuture =
            connection.async().jsonGet(docId, JsonPath.of("$"))
                .toCompletableFuture()
                .thenApply(jsonValue -> {
                  String retrievedJson = jsonValue.get(0).asJsonArray().getFirst().toString();
                  HashMap<String, ByteIterator> values = new HashMap<>();
                  try {
                    values = (HashMap<String, ByteIterator>) mapper.readValue(retrievedJson, typeRef);
                  } catch (Exception e) {
                    logger.error("Error during JSON get: {}", e.getMessage(), e);
                  }
                  return values;
                });

        futures.add(fetchFuture);
      }

      CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();

      for (CompletableFuture<HashMap<String, ByteIterator>> f : futures) {
        result.add(f.join());
      }

      return Status.OK;
    } catch (Exception e) {
      logger.error("Error during JSON scan: {}", e.getMessage(), e);
      return Status.ERROR;
    }
  }
}
