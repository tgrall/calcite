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

import org.apache.calcite.linq4j.Enumerator;

import org.apache.calcite.linq4j.Linq4j;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Protocol;

import java.util.List;

public class RedisearchEnumator implements Enumerator<Object[]> {
  private final Enumerator<Object[]> enumerator;



  private final int maxTotal = GenericObjectPoolConfig.DEFAULT_MAX_TOTAL;
  private final int maxIdle = GenericObjectPoolConfig.DEFAULT_MAX_IDLE;
  private final int minIdle = GenericObjectPoolConfig.DEFAULT_MIN_IDLE;
  private final int timeout = Protocol.DEFAULT_TIMEOUT;

  private final RowConverter<Object[]> rowConverter = null;
  private Object[] current;



  public RedisearchEnumator(RedisConfig redisConfig, RedisearchSchema schema, String tableName, String indexName) {
    System.out.println("RedisearchEnumator.RedisearchEnumator() "+
        "\n\t redisConfig " + redisConfig +
        "\n\t indexName " + indexName
    );

    RedisJedisManager redisManager = new RedisJedisManager(redisConfig.getHost(),
        redisConfig.getPort(), redisConfig.getDatabase(), redisConfig.getPassword(), indexName);

    RedisearchDataProcess dataProcess = new RedisearchDataProcess(redisManager.getRediSearchClient());
    List<Object[]> objs = dataProcess.read();
    enumerator = Linq4j.enumerator(objs);



  }

  @Override public Object[] current() {
    System.out.println("RowConverter.current");
    return enumerator.current();
  }

  @Override public boolean moveNext() {
    System.out.println("RowConverter.moveNext");
    return enumerator.moveNext();
  }

  @Override public void reset() {
    System.out.println("RowConverter.reset");
    enumerator.reset();
  }

  @Override public void close() {
    System.out.println("RowConverter.close");
    enumerator.close();
  }

  public static RowConverter<Object[]> getConverter() {
    System.out.println("RowConverter.getConverter");
    return new  RowConverter();
  }

  static class RowConverter<E> {

    protected Object convert() {
      System.out.println("RowConverter.convert");
      return null;
    }

  }

}
