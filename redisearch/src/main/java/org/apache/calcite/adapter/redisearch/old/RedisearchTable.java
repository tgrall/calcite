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


import io.redisearch.Query;
import io.redisearch.SearchResult;
import io.redisearch.client.Client;

import org.apache.calcite.adapter.java.AbstractQueryableTable;
import org.apache.calcite.linq4j.*;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.impl.AbstractTableQueryable;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.Map;

public class RedisearchTable extends AbstractQueryableTable
    implements TranslatableTable {

  final RedisearchSchema schema;
  final String tableName; // Table name used in the SQL statement
  final String indexName; // RediSearch Index name used to query Redis
  final RelProtoDataType protoRowType;
  final RedisConfig redisConfig;

  RedisearchTable(String indexName) {
    super(Object[].class);
    System.out.println("RedisearchTable() 1 "+ indexName);
    this.schema = null;
    this.tableName = null;
    this.indexName = indexName;
    this.protoRowType = null;
    this.redisConfig = null;
  }

  public RedisearchTable(
      RedisearchSchema schema,
      String tableName,
      String indexName,
      RelProtoDataType protoRowType,
      RedisConfig redisConfig) {
    super(Object[].class);
    System.out.println("RedisearchTable() 2 "+ indexName);
    this.schema = schema;
    this.tableName = tableName;
    this.indexName = indexName;
    this.protoRowType = protoRowType;
    this.redisConfig = redisConfig;
  }



  @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    final RelDataType mapType =
        typeFactory.createMapType(
            typeFactory.createSqlType(SqlTypeName.VARCHAR),
            typeFactory.createTypeWithNullability(
                typeFactory.createSqlType(SqlTypeName.ANY), true));

    System.out.println("RedisearchTable.getRowType ==== "+ mapType);
    return typeFactory.builder().add("_MAP", mapType).build();


    // TODO : how to get the proper schema
//    return typeFactory.builder()
//        .add("key", mapType)
//        .add("imdb", mapType)
//        .add("plot", mapType)
//        .add("release_year", mapType)
//        .add("genre", mapType)
//        .add("ratings", mapType)
//        .add("votes", mapType)
//        .add("title", mapType)
//        .build();
  }

  static Table create(
      RedisearchSchema schema,
      String tableName,
      String indexName,
      RedisConfig redisConfig,
      RelProtoDataType protoRowType) {
    System.out.println("RedisearchTable.create ==== ");

    return new RedisearchTable(schema, tableName, indexName, protoRowType, redisConfig);
  }

  static Table create(
      RedisearchSchema schema,
      String tableName,
      Map operand,
      RelProtoDataType protoRowType) {
    System.out.println("RedisearchTable.create  2==== ");

    RedisConfig redisConfig = new RedisConfig(schema.host, schema.port, schema.database, schema.password);
    return create(schema, tableName, schema.getRediSearchIndexName(tableName), redisConfig, protoRowType);
  }





  @Override public String toString() {
    return "RedisearchTable{" + "schema=" + schema + ", tableName='" + tableName + '\''
        + ", indexName='" + indexName + '\'' + ", protoRowType=" + protoRowType + ", redisConfig="
        + redisConfig + '}';
  }

  @Override public <T> Queryable<T> asQueryable(QueryProvider queryProvider, SchemaPlus schema,
      String tableName) {
    System.out.println("RedisearchTable.asQueryable");

    return null;
  }

  @Override public RelNode toRel(
      RelOptTable.ToRelContext context,
      RelOptTable relOptTable) {
    System.out.println("RedisearchTable.toRel");

    final RelOptCluster cluster = context.getCluster();
    return new RediSearchTableScan(cluster, cluster.traitSetOf(RedisearchRel.CONVENTION),
        relOptTable, this, null);
  }


  private Enumerable<Object> query() {
    System.out.println("RediSearchTable.query()");

    Client c = new Client("idx:movie", "localhost", 6379);

    Query q = new Query("*");
    SearchResult result = c.search(q);

    System.out.println(result.docs);

    return null;


  }

  public static class RedisearchQueryable<T> extends AbstractTableQueryable<T> {

    RedisearchQueryable(QueryProvider queryProvider, SchemaPlus schema,
        RedisearchTable table, String tableName) {
      super(queryProvider, schema, table, tableName);
      System.out.println("RedisearchQueryable.RedisearchQueryable");

    }

    @Override public Enumerator<T> enumerator() {
      //noinspection unchecked
      final Enumerable<T> enumerable =
          (Enumerable<T>) getTable().query();
      return enumerable.enumerator();
    }

    private RedisearchTable getTable() {
      return (RedisearchTable) table;
    }

  }



}
