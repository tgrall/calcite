/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.adapter.redisearch;


import io.redisearch.Schema;
import io.redisearch.client.Client;
import io.redisearch.client.IndexDefinition;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.util.HashMap;
import java.util.Map;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * RedisDataTypeTest.
 */
public class RedisearchDataCaseBase extends RedisearchCaseBase {
  protected JedisPool pool;

  @BeforeEach
  public void setUp() {
    try {
      JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
      jedisPoolConfig.setMaxTotal(10);
      pool = new JedisPool(jedisPoolConfig, getRedisServerHost(), getRedisServerPort());

      // Flush all data
      try (Jedis jedis = pool.getResource()) {
        jedis.flushAll();
      }

    } catch (Exception e) {
      throw e;
    }
  }

  public void makeData() {
    try (Jedis jedis = pool.getResource()) {

      // Create an index
      Schema schema = new Schema()
          .addTextField("title", 5.0)
          .addSortableTagField("genre", ",")
          .addNumericField("release_year");

      IndexDefinition indexDefinition = new IndexDefinition()
          .setPrefixes(new String[]{"movie:"});

      Client client = new Client("idx:movie", pool);
      client.createIndex(schema,
          Client.IndexOptions.defaultOptions().setDefinition(indexDefinition));


      // Create data
      Map<String, String> movie = new HashMap<>();
      movie.put("title", "King of New York");
      movie.put("genre", "Crime");
      movie.put("release_year", "1990");
      movie.put("rating", "7.0");
      movie.put("votes", "29432");
      jedis.hset("movie:001", movie);

      movie = new HashMap<>();
      movie.put("title", "Star Wars: Episode I - The Phantom Menace");
      movie.put("genre", "Action");
      movie.put("release_year", "1990");
      movie.put("rating", "6.5");
      movie.put("votes", "698056");
      jedis.hset("movie:002", movie);

      movie = new HashMap<>();
      movie.put("title", "Star Wars: Episode II - Attack of the Clones");
      movie.put("genre", "Action");
      movie.put("release_year", "2002");
      movie.put("rating", "6.5");
      movie.put("votes", "618036");
      jedis.hset("movie:003", movie);

      movie = new HashMap<>();
      movie.put("title", "Star Wars: Episode III - Revenge of the Sith");
      movie.put("genre", "Action");
      movie.put("release_year", "2005");
      movie.put("rating", "7.5");
      movie.put("votes", "679858");
      jedis.hset("movie:004", movie);

      movie = new HashMap<>();
      movie.put("title", "Star Wars: Episode IV - A New Hope");
      movie.put("genre", "Action");
      movie.put("release_year", "1977");
      movie.put("rating", "8.6");
      movie.put("votes", "1181515");
      jedis.hset("movie:005", movie);
    }
  }

  @AfterEach
  public void shutDown() {
    if (null != pool) {
      pool.destroy();
    }
  }
}
