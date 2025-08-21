package com.redislabs.ycsb;

import com.codelry.util.ycsb.ByteIterator;
import com.codelry.util.ycsb.Status;
import com.codelry.util.ycsb.StringByteIterator;

import com.redis.lettucemod.api.async.RedisModulesAsyncCommands;
import com.redis.lettucemod.api.sync.RedisModulesCommands;
import io.lettuce.core.KeyValue;
import io.lettuce.core.search.SearchReply;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HashSearchRecordStore implements RecordStore {
  private static final Logger logger = LoggerFactory.getLogger(HashSearchRecordStore.class);

  private final RedisModulesCommands<String, String> mod;
  private final RedisModulesAsyncCommands<String, String> asyncMod;
  private final String INDEX_NAME;

  HashSearchRecordStore(RedisModulesCommands<String, String> mod, RedisModulesAsyncCommands<String, String> asyncMod, String indexName) {
    this.mod = mod;
    this.asyncMod = asyncMod;
    this.INDEX_NAME = indexName;
  }

  private String keyNumber(String key) {
    return key.substring(4);
  }

  @Override
  public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    if (fields == null) {
      StringByteIterator.putAllAsByteIterators(result, mod.hgetall(key));
    } else {
      String[] fieldArray = fields.toArray(new String[0]);
      List<KeyValue<String, String>> values = mod.hmget(key, fieldArray);
      for (KeyValue<String, String> entry : values) {
        result.put(entry.getKey(), new StringByteIterator(entry.getValue()));
      }
    }
    return result.isEmpty() ? Status.ERROR : Status.OK;
  }

  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    try {
      Map<String, String> map = StringByteIterator.getStringMap(values);
      map.put("id", keyNumber(key));
      return mod.hmset(key, map).equals("OK") ? Status.OK : Status.ERROR;
    } catch (Exception e) {
      logger.error("Error during Hash set: {}", e.getMessage(), e);
      return Status.ERROR;
    }
  }

  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {
    try {
      return insert(table, key, values);
    } catch (Exception e) {
      logger.error("Error during Hash update: {}", e.getMessage(), e);
      return Status.ERROR;
    }
  }

  @Override
  public Status delete(String table, String key) {
    return mod.del(key).equals(0L) ? Status.ERROR : Status.OK;
  }

  @Override
  public Status scan(String table, String key, int count, Set<String> fields,
                     Vector<HashMap<String, ByteIterator>> result) {
    try {
      String query = String.format("@id:[%s +inf] LIMIT 0 %d", keyNumber(key), count);

      CompletableFuture<SearchReply<String, String>> searchFuture =
          asyncMod.ftSearch(INDEX_NAME, query).toCompletableFuture();

      SearchReply<String, String> searchResult = searchFuture.join();

      List<CompletableFuture<HashMap<String, ByteIterator>>> futures = new ArrayList<>();

      for (SearchReply.SearchResult<String, String> r : searchResult.getResults()) {
        String docId = r.getId();

        CompletableFuture<HashMap<String, ByteIterator>> fetchFuture;
        if (fields == null) {
          fetchFuture =
              asyncMod.hgetall(docId)
                  .toCompletableFuture()
                  .thenApply(map -> {
                    HashMap<String, ByteIterator> values = new HashMap<>(map.size());
                    StringByteIterator.putAllAsByteIterators(values, map);
                    return values;
                  });
        } else {
          fetchFuture =
              asyncMod.hgetall(docId)
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
