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

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeImpl;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.TableFactory;

import java.util.Map;

/**
 * TODO: document.
 */
public class RedisearchTableFactory implements TableFactory {

  public static final RedisearchTableFactory INSTANCE = new RedisearchTableFactory();

  RedisearchSchema redisSchema = null;
  String tableName = null;
  String indexName = null;

  private RedisearchTableFactory() {

  }

  @Override public Table create(SchemaPlus schema, String name, Map operand, RelDataType rowType) {

    System.out.println("RedisearchTableFactory.RedisearchTableFactory() "
        + "\n\tschema " + schema
        + "\n\tname " + name
        + "\n\toperand " + operand
        + "\n\trowType " + rowType
    );
    System.out.println("RedisearchTableFactory.RedisearchTableFactory() -001 ");


    //    String host = operand.get("host").toString();
    //    int port = (int) operand.get("port");
    //    int database = Integer.parseInt(operand.get("database").toString());
    //    String password = operand.get("password") == null ? null : operand.get("password")
    //    .toString();
    //    @SuppressWarnings("unchecked") List<Map<String, Object>> tables = (List) operand.get
    //    ("tables");
    //
    //    this.tableName = name;
    //    this.indexName = (String)operand.get("indexName");
    //
    System.out.println("RedisearchTableFactory.RedisearchTableFactory() -002 ");


    final RedisearchSchema redisSchema = schema.unwrap(RedisearchSchema.class);
    final RelProtoDataType protoRowType =
        rowType != null ? RelDataTypeImpl.proto(rowType) : null;


    //
    //    RedisearchSchema redisearchSchema = new RedisearchSchema(host, port, database,
    //    password, tables);
    //    System.out.println("RedisearchTableFactory.RedisearchTableFactory() -003 ");

    return new RedisearchTable(redisSchema, name, operand, rowType);


  }
}
