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

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.calcite.config.CalciteSystemProperty;
import org.apache.calcite.test.CalciteAssert;

import org.apache.calcite.util.Sources;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Protocol;

import java.io.File;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class RedisearchAdapterCaseBase extends RedisearchDataCaseBase {

  private String filePath =
      Sources.of(RedisearchCaseBase.class.getResource("/redisearch-model.json"))
          .file().getAbsolutePath();

  private String model;

  @BeforeEach
  @Override public void makeData() {
    super.makeData();
    readModelByJson();
    System.out.println(model);
  }

  /**
   * Whether to run this test.
   */
  private boolean enabled() {
    return CalciteSystemProperty.TEST_REDIS.value();
  }

  @Test void testRedisBySql() {

    try (Jedis jedis = pool.getResource()) {
      System.out.println(
          jedis.hgetAll("movie:001")
      );
    }

    String sql = "select cast(_MAP['title'] AS varchar(120)) AS \"title\" from movies";

    Connection connection = null;
    try {

      Properties info = new Properties();
      info.put("model", Sources.of(RedisearchCaseBase.class.getResource("/redisearch-model.json"))
          .file().getAbsolutePath());
      connection = DriverManager.getConnection("jdbc:calcite:", info);


      Statement statement = connection.createStatement();
      ResultSet rs = statement.executeQuery(sql);

      ResultSetMetaData rsmd = rs.getMetaData();
      List<String> columns = new ArrayList<String>(rsmd.getColumnCount());
      for(int i = 1; i <= rsmd.getColumnCount(); i++){
        columns.add(rsmd.getColumnName(i));
      }

      while(rs.next()){
        for(String col : columns) {
          System.out.println("\t"+ col +" : "+ rs.getString(col));
        }
        System.out.println("\t--- --- ---");
      }

      rs.close();
      statement.close();


    } catch (Exception e) {
      e.printStackTrace();
    } finally {
    }




  }


  private void readModelByJson() {
    String strResult = null;
    try {
      ObjectMapper objMapper = new ObjectMapper();
      objMapper.configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true)
          .configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true)
          .configure(JsonParser.Feature.ALLOW_COMMENTS, true);
      File file = new File(filePath);
      if (file.exists()) {
        JsonNode rootNode = objMapper.readTree(file);
        strResult = rootNode.toString().replace(Integer.toString(Protocol.DEFAULT_PORT),
            Integer.toString(getRedisServerPort()));
      }
    } catch (Exception ignored) {
    }
    model = strResult;
  }

}
