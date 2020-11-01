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
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.type.SqlTypeName;

import io.redisearch.Document;
import io.redisearch.Query;
import io.redisearch.SearchResult;
import io.redisearch.client.Client;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * TODO : to document.
 */
public class RedisearchDataProcess {

  private Client redisearchClient;


  public RedisearchDataProcess(Client redisearchClient) {
    System.out.println("RedisearchDataProcess.RedisearchDataProcess() "
        + "\n\t redisearchClient " + redisearchClient
    );
    this.redisearchClient = redisearchClient;
  }

  public List<Map<String, Object>> read(String queryString) {
    System.out.println("RedisearchDataProcess.read() ");

    List<Map<String, Object>> objs = new ArrayList<>();

    if (queryString == null || queryString.isEmpty()) {
      queryString = "*";
    }

    Query q = new Query(queryString);
    SearchResult queryResult = redisearchClient.search(q);

    List<Document> docs = queryResult.docs;
    for (Document doc : docs) {
      Map<String, Object> row = new HashMap<>();

      //meta.put("id", doc.getId());
      //meta.put("score", doc.getScore());
      row.put("key", doc.getId());
      doc.getProperties().forEach(e -> {
        row.put(e.getKey(), e.getValue());
      });
      objs.add(row);

    }
    return objs;
  }


  /**
   * call the search to return the list of field (and eventually their type.
   * @param typeFactory to generate the proper datatype for the schema.
   * @return
   */
  public Map<String, RelDataType> getRowTypeFromData(RelDataTypeFactory typeFactory) {
    System.out.println("RedisearchDataProcess.getRowTypeFromData() ");
    Map<String, RelDataType> fieldsInfo = new HashMap<>();

    Query q = new Query("*").limit(0, 1);
    SearchResult queryResult = redisearchClient.search(q);
    List<Document> docs = queryResult.docs;
    for (Document doc : docs) {
      doc.getProperties().forEach(e -> {
        fieldsInfo.put(e.getKey(), typeFactory.createSqlType(SqlTypeName.VARCHAR));
      });
    }
    return fieldsInfo;
  }


}
