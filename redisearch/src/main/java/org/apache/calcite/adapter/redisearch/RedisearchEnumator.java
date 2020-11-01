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

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.linq4j.Enumerator;

import org.apache.calcite.linq4j.Linq4j;

import org.apache.calcite.rel.type.RelDataType;

import org.apache.calcite.util.Pair;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import redis.clients.jedis.Protocol;

import javax.naming.spi.ObjectFactory;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class RedisearchEnumator implements Enumerator<Object[]> {

  private final Enumerator<Object[]> enumerator;
  private final RedisearchDataProcess dataProcess;
  private Object[] current;

  List<Map<String, Object>> objs ;
  Iterator iterator;



  private final int maxTotal = GenericObjectPoolConfig.DEFAULT_MAX_TOTAL;
  private final int maxIdle = GenericObjectPoolConfig.DEFAULT_MAX_IDLE;
  private final int minIdle = GenericObjectPoolConfig.DEFAULT_MIN_IDLE;
  private final int timeout = Protocol.DEFAULT_TIMEOUT;



  public RedisearchEnumator(
      RedisConfig redisConfig,
      RedisearchSchema schema,
      String tableName,
      String indexName,
      String queryString
  ) {
    System.out.println("RedisearchEnumator.RedisearchEnumator() "+
        "\n\t redisConfig " + redisConfig +
        "\n\t tableName " + tableName +
        "\n\t indexName " + indexName +
        "\n\t queryString " + queryString
    );

    RedisJedisManager redisManager = new RedisJedisManager(redisConfig.getHost(),
        redisConfig.getPort(), redisConfig.getDatabase(), redisConfig.getPassword(), indexName);

    this.dataProcess = new RedisearchDataProcess(redisManager.getRediSearchClient());
    objs = dataProcess.read(queryString);
    iterator = objs.iterator();

    // TODO : clean this when dynamic schema is supported
    System.out.println("RedisearchEnumator.RedisearchEnumator() => "+  objs );
    List<Object[]> newObjs = new ArrayList<>();
    for (Object row : objs) {
      newObjs.add(new Object[]{row});
    }

    enumerator = Linq4j.enumerator(newObjs);

  }

  @Override public Object[] current() {
 //   System.out.println("RedisearchEnumator.current");
    return enumerator.current();
  }

  @Override public boolean moveNext() {
//    System.out.println("RedisearchEnumator.moveNext");
    return enumerator.moveNext();
  }

  @Override public void reset() {
    System.out.println("RedisearchEnumator.reset");
    enumerator.reset();
  }

  @Override public void close() {
    System.out.println("RedisearchEnumator.close");
    enumerator.close();
  }

  /**
   * Deduce the name of the fields by calling search on a single row
   * TODO :
   *  - use the index definition to extract some datatype
   *  - see if a better approach is possible
   *  - add support for explicit definition in the table
   *
   * @param typeFactory
   * @return
   */
  public RelDataType deduceRowType(JavaTypeFactory typeFactory) {
    System.out.println("RedisearchEnumator.deduceRowType");
    final List<String> names = new ArrayList<>();
    final List<RelDataType> types = new ArrayList<>();

    Map<String, RelDataType> sampleRow =  dataProcess.getRowTypeFromData(typeFactory);

    return typeFactory.createStructType(Pair.zip(names, types));
  }

}
