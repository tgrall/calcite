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

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;

import org.apache.calcite.model.JsonCustomTable;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class RedisearchSchema extends AbstractSchema {

  public final String host;
  public final int port;
  public final int database;
  public final String password;
  public final List<Map<String, Object>> tables;
  private Map<String, Table> tableMap = null;

  RedisearchSchema(String host,
      int port,
      int database,
      String password,
      List<Map<String, Object>> tables) {
    this.host = host;
    this.port = port;
    this.database = database;
    this.password = password;
    this.tables = tables;

    System.out.println("RedisearchSchema "+ tables);

  }


  @Override protected Map<String, Table> getTableMap() {
    JsonCustomTable[] jsonCustomTables = new JsonCustomTable[tables.size()];
    Set<String> tableNames = Arrays.stream(tables.toArray(jsonCustomTables))
        .map(e -> e.name).collect(Collectors.toSet());
    tableMap = Maps.asMap(
        ImmutableSet.copyOf(tableNames),
        CacheBuilder.newBuilder()
            .build(CacheLoader.from(this::table)));
    System.out.println("getTableMap "+ tableMap);
    return tableMap;
  }

  private Table table(String tableName) {
    RedisConfig redisConfig = new RedisConfig(host, port, database, password);

    String indexName = getRediSearchIndexName(tableName);

    return RedisearchTable.create(RedisearchSchema.this, tableName, indexName, redisConfig, null);
  }

  /**
   * Extract the RediSearch Index Name associated to the table definition
   * If the index name is not specified return use the tableName
   * @param tableName
   * @return
   */
  public String getRediSearchIndexName(String tableName) {
    String indexName = tableName;
    Map<String, Object> map;
    for (int i = 0; i < this.tables.size(); i++) {
      JsonCustomTable jsonCustomTable = (JsonCustomTable) this.tables.get(i);
      if (jsonCustomTable.name.equals(tableName)) {
        map = jsonCustomTable.operand;
        if (map.get("indexName") != null) {
          indexName = (String)map.get("indexName");
        }
        System.out.println("INDEX NAME "+ indexName);
        break;
      }

      }

      return  indexName;
  }

}
