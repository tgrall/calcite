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

import org.apache.calcite.util.Sources;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class FooBar {

  private String filePath =
      Sources.of(RedisearchCaseBase.class.getResource("/redisearch-model.json"))
          .file().getAbsolutePath();

  private String model;

  public static void main(String[] args) {

    System.out.println("TUG");


    JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
    jedisPoolConfig.setMaxTotal(10);
    JedisPool pool = new JedisPool(jedisPoolConfig,  "localhost", 6379);

//    try (Jedis jedis = pool.getResource()) {
//      System.out.println(
//          jedis.hgetAll("movie:001")
//      );
//    }

//    String sql = "SELECT 'title', "
//        + "'genre' "
//        //+ " cast('key' AS  varchar(30)) AS \"key\" "
//        + " FROM movies "
//         // + " WHERE 'genre' = 'Action'"
//        ;

        String sql = "SELECT cast(_MAP['title'] AS varchar(120)) AS \"title\", "
            + " cast(_MAP['genre'] AS varchar(30)) AS \"genre\", "
            + " cast(_MAP['rating'] AS double) AS \"rating\", "
        + " cast(_MAP['key'] AS  varchar(30)) AS \"key\" "
        + " FROM movies "
        + " WHERE _MAP['genre'] = 'Action'"
        ;

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

}
