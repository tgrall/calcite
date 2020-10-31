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

package org.apache.calcite.adapter.redisearch.old;

import io.redisearch.client.Client;

import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.Linq4j;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.List;

public class RedisearchEnumerator implements Enumerator {
  private final Enumerator<Object[]> enumerator;
  private final String tableName;
  private final String indexName;


  RedisearchEnumerator(RedisConfig redisConfig, RedisearchSchema schema, String tableName, String indexName) {

    System.out.println("RedisearchEnumerator() 1");

    this.tableName = tableName;
    this.indexName = indexName;
    RedisJedisManager redisManager = new RedisJedisManager(redisConfig.getHost(),
        redisConfig.getPort(), redisConfig.getDatabase(), redisConfig.getPassword());System.out.println("RedisearchEnumerator");


    JedisPool pool = redisManager.getJedisPool();
    try (Jedis j = pool.getResource()) {
      System.out.println("TUG ===== "+ j.hgetAll("movie:001"));
    }


    Client redisearchClient =  new Client(indexName,pool);
    RedisearchDataProcess dataProcess = new RedisearchDataProcess(redisearchClient, tableName, indexName);
    List<Object[]> objs = dataProcess.read();

    System.out.println(" FIRST LINE === === ===");
    System.out.println( objs.get(1).length );

    enumerator = Linq4j.enumerator(objs);


    }

  @Override public Object[] current() {
    System.out.println("RedisearchEnumerator.current");

    return enumerator.current();
  }

  @Override public boolean moveNext() {
    System.out.println("RedisearchEnumerator.moveNext");

    return enumerator.moveNext();
  }

  @Override public void reset() {
    System.out.println("RedisearchEnumerator.reset");

    enumerator.reset();
  }

  @Override public void close() {
    System.out.println("RedisearchEnumerator.close");

    enumerator.close();
  }
}
