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

import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;

import java.util.List;
import java.util.Map;

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

    System.out.println("RedisearchSchema.RedisearchSchema "+
        "\n\t host "+ host +
        "\n\t port "+ port +
        "\n\t database "+ database +
        "\n\t password "+ password +
        "\n\t tables "+ tables
        );

    this.host = host;
    this.port = port;
    this.database = database;
    this.password = password;
    this.tables = tables;

  }

  @Override protected Map<String, Table> getTableMap() {
    System.out.println("RedisearchSchema.getTableMap");
    return super.getTableMap();
  }

  @Override public String toString() {
    return "RedisearchSchema{" + "host='" + host + '\'' + ", port=" + port + ", database="
        + database + ", password='" + password + '\'' + ", tables=" + tables + ", tableMap="
        + tableMap + '}';
  }
}
