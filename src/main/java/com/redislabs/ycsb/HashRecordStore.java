package com.redislabs.ycsb;

import com.codelry.util.ycsb.ByteIterator;
import com.codelry.util.ycsb.Status;
import com.codelry.util.ycsb.StringByteIterator;

import io.lettuce.core.KeyValue;
import io.lettuce.core.Range;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.api.sync.RedisCommands;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HashRecordStore implements RecordStore {
  private static final Logger logger = LoggerFactory.getLogger(HashRecordStore.class);

  private static final AtomicLong COUNTER = new AtomicLong(0);

  private final RedisCommands<String, String> sync;
  private final RedisAsyncCommands<String, String> async;
  private final String INDEX_KEY;

  HashRecordStore(RedisCommands<String, String> sync, RedisAsyncCommands<String, String> async, String indexName) {
    this.sync = sync;
    this.async = async;
    this.INDEX_KEY = indexName;
  }

  public void addKeyToIndex(String key) {
    sync.zadd(INDEX_KEY, COUNTER.incrementAndGet(), key);
  }

  public void removeKeyFromIndex(String key) {
    sync.zrem(INDEX_KEY, key);
  }

  public List<String> scanKeys(double id, int count) {
    return sync.zrangebyscore(INDEX_KEY, Range.create(id, id + count - 1));
  }

  @Override
  public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    try {
      if (fields == null) {
        StringByteIterator.putAllAsByteIterators(result, sync.hgetall(key));
      } else {
        String[] fieldArray = fields.toArray(new String[0]);
        List<KeyValue<String, String>> values = sync.hmget(key, fieldArray);
        for (KeyValue<String, String> entry : values) {
          result.put(entry.getKey(), new StringByteIterator(entry.getValue()));
        }
      }
      return result.isEmpty() ? Status.ERROR : Status.OK;
    } catch (Exception e) {
      logger.error("Error during Hash read: {}", e.getMessage(), e);
      return Status.ERROR;
    }
  }

  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    try {
      if (sync.hmset(key, StringByteIterator.getStringMap(values)).equals("OK")) {
        addKeyToIndex(key);
        return Status.OK;
      } else {
        return Status.ERROR;
      }
    } catch (Exception e) {
      logger.error("Error during Hash insert: {}", e.getMessage(), e);
      return Status.ERROR;
    }
  }

  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {
    try {
      return sync.hmset(key, StringByteIterator.getStringMap(values)).equals("OK") ? Status.OK : Status.ERROR;
    } catch (Exception e) {
      logger.error("Error during Hash update: {}", e.getMessage(), e);
      return Status.ERROR;
    }
  }

  @Override
  public Status delete(String table, String key) {
    if (sync.del(key).equals(0L)) {
      return Status.ERROR;
    } else {
      removeKeyFromIndex(key);
      return Status.OK;
    }
  }

  @Override
  public Status scan(String table, String key, int count, Set<String> fields,
                     Vector<HashMap<String, ByteIterator>> result) {
    double id = sync.zscore(INDEX_KEY, key);
    List<String> keys = scanKeys(id, count);

    if (keys.isEmpty()) {
      return Status.OK;
    }

    try {
      List<CompletableFuture<HashMap<String, ByteIterator>>> futures = new ArrayList<>(keys.size());

      for (String k : keys) {
        if (fields == null) {
          futures.add(
              async.hgetall(k).toCompletableFuture()
                  .thenApply(map -> {
                    HashMap<String, ByteIterator> values = new HashMap<>(map.size());
                    StringByteIterator.putAllAsByteIterators(values, map);
                    return values;
                  })
          );
        } else {
          futures.add(
              async.hgetall(k).toCompletableFuture()
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
      logger.error("Error during scan: {}", e.getMessage(), e);
      return Status.ERROR;
    }
  }
}
