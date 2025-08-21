package com.redislabs.ycsb;

import com.codelry.util.ycsb.ByteIterator;
import com.codelry.util.ycsb.Status;
import com.codelry.util.ycsb.StringByteIterator;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.node.ObjectNode;

import com.redis.lettucemod.api.async.RedisModulesAsyncCommands;
import com.redis.lettucemod.api.sync.RedisModulesCommands;
import io.lettuce.core.json.JsonPath;
import io.lettuce.core.search.SearchReply;

import java.util.*;
import java.util.concurrent.CompletableFuture;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JsonRecordStore implements RecordStore {
  private static final Logger logger = LoggerFactory.getLogger(JsonRecordStore.class);

  private final RedisModulesCommands<String, String> json;
  private final RedisModulesAsyncCommands<String, String> asyncJson;
  private final ObjectMapper mapper = new ObjectMapper();
  private final TypeReference<Map<String, ByteIterator>> typeRef = new TypeReference<Map<String, ByteIterator>>() {};
  private final String INDEX_NAME;

  JsonRecordStore(RedisModulesCommands<String, String> json, RedisModulesAsyncCommands<String, String> asyncJson, String indexName) {
    this.json = json;
    this.asyncJson = asyncJson;
    this.INDEX_NAME = indexName;
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
  public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    try {
      if (fields == null) {
        String value = json.jsonGet(key, JsonPath.of("$")).get(0).asJsonArray().getFirst().toString();
        result = mapper.readValue(value, typeRef);
      } else {
        for (String f : fields) {
          String field = json.jsonGet(key, JsonPath.of("$." + f)).toString();
          result.put(f, new StringByteIterator(field));
        }
      }
      return result.isEmpty() ? Status.ERROR : Status.OK;
    } catch (Exception e) {
      logger.error("Error during JSON read: {}", e.getMessage(), e);
      return Status.ERROR;
    }
  }

  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    try {
      byte[] jsonBytes = mapper.writeValueAsBytes(values);
      ObjectNode jsonData = (ObjectNode) mapper.readTree(jsonBytes);
      jsonData.put("id", hashKey(key));
      String jsonString = mapper.writeValueAsString(jsonData);
      String res = json.jsonSet(key, JsonPath.of("$"), json.getJsonParser().createJsonValue(jsonString));
      return "OK".equalsIgnoreCase(res) ? Status.OK : Status.ERROR;
    } catch (Exception e) {
      logger.error("Error during JSON insert: {}", e.getMessage(), e);
      return Status.ERROR;
    }
  }

  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {
    try {
      return insert(table, key, values);
    } catch (Exception e) {
      return Status.ERROR;
    }
  }

  @Override
  public Status delete(String table, String key) {
    return json.del(key).equals(0L) ? Status.ERROR : Status.OK;
  }

  @Override
  public Status scan(String table, String key, int count, Set<String> fields,
                     Vector<HashMap<String, ByteIterator>> result) {
    try {
      String query = String.format("@id:[%d +inf] LIMIT 0 %d", key.hashCode(), count);

      CompletableFuture<SearchReply<String, String>> searchFuture =
          asyncJson.ftSearch(INDEX_NAME, query).toCompletableFuture();

      SearchReply<String, String> searchResult = searchFuture.join();

      List<CompletableFuture<HashMap<String, ByteIterator>>> futures = new ArrayList<>();

      for (SearchReply.SearchResult<String, String> r : searchResult.getResults()) {
        String docId = r.getId();

        CompletableFuture<HashMap<String, ByteIterator>> fetchFuture =
            asyncJson.jsonGet(docId, JsonPath.of("$"))
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
