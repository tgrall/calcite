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

import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;

import java.util.List;
import java.util.Map;

/**
 * TODO : document.
 */
public class RedisearchSchemaFactory implements SchemaFactory {

  /** Public singleton, per factory contract. */
  public static final RedisearchSchemaFactory INSTANCE = new RedisearchSchemaFactory();

  private RedisearchSchemaFactory() {
    System.out.println("RedisearchSchemaFactory.RedisearchSchemaFactory() ");
  }


  @Override public Schema create(SchemaPlus parentSchema, String name,
      Map<String, Object> operand) {
    System.out.println("RedisearchSchemaFactory.create() "
        + "\n\tparentSchema " + parentSchema
        + "\n\tname " + name
        + "\n\toperand " + operand
    );

    @SuppressWarnings("unchecked")
    List<Map<String, Object>> tables = (List) operand.get("tables");
    String host = operand.get("host").toString();
    int port = (int) operand.get("port");
    int database = Integer.parseInt(operand.get("database").toString());
    String password = operand.get("password") == null ? null : operand.get("password").toString();

    return new RedisearchSchema(host, port, database, password, tables);
  }

}
