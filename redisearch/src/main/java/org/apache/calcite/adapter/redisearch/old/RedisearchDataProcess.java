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

import io.redisearch.Document;
import io.redisearch.Query;
import io.redisearch.SearchResult;
import io.redisearch.client.Client;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RedisearchDataProcess {

  String tableName;
  String indexName;
  private Client redisearchClient;

  public RedisearchDataProcess(Client client, String tableName, String indexName) {

    System.out.println("CLIENT "+ client);

    this.redisearchClient = client;
    this.tableName = tableName;
    this.indexName = indexName;
  }

  public List<Object[]> read() {
    List<Object[]> objs = new ArrayList<>();

    // Search all documents
    Query q = new Query("*");
    SearchResult searchResult = redisearchClient.search(q);

    List<Document> docs =  searchResult.docs;

    for (Document doc :docs) {
      List fieldValues = new ArrayList();
      fieldValues.add(doc.getId());
      doc.getProperties().forEach( e -> {
        fieldValues.add(e.getValue()); // TODO : optimize
      });
      objs.add(fieldValues.toArray());
    }

    return  objs;

  }


}
