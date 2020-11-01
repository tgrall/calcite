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

import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.FilterableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.List;
import java.util.Map;

/**
 * Mapping of the redisearch table (redisearch index).
 */
public class RedisearchTable extends AbstractTable implements FilterableTable {

  //  protected final Source source;
  private RelDataType rowType;

  final RedisearchSchema schema;
  final String tableName;
  final String indexName;
  final RelProtoDataType protoRowType;
  final RedisConfig redisConfig;
  private final RedisearchDataProcess dataProcess;


  public RedisearchTable(RedisearchSchema schema, String tableName, Map<String, Object> operand,
      RelDataType rowType) {
    System.out.println("RedisearchTable.RedisearchTable()  "
        + "\n\t schema " + schema
        + "\n\t tableName " + tableName
        + "\n\t operand " + operand
        + "\n\t rowType " + rowType
        + "\n\t operand indexName " + operand.get("indexName")
    );

    this.schema = schema;
    this.tableName = tableName;
    this.indexName = (String) operand.get("indexName");
    this.protoRowType = null; // TODO : Implement


    this.redisConfig = new RedisConfig(schema.host, schema.port, schema.database, schema.password);

    // TODO : see to have a single one for table & enumerator
    RedisJedisManager redisManager = new RedisJedisManager(redisConfig.getHost(),
        redisConfig.getPort(), redisConfig.getDatabase(), redisConfig.getPassword(), indexName);
    this.dataProcess = new RedisearchDataProcess(redisManager.getRediSearchClient());

  }

  @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    System.out.println("RedisearchTable.getRowType() " + typeFactory);


    // TOD: Make schema as MAP available too
    final RelDataType mapType =
        typeFactory.createMapType(
            typeFactory.createSqlType(SqlTypeName.VARCHAR),
            typeFactory.createTypeWithNullability(
                typeFactory.createSqlType(SqlTypeName.ANY), true));


    return typeFactory.builder().add("_MAP", mapType).build();

//    // TODO : dynamic schema (I do not know how to use it in Enumerator
//    Map<String, RelDataType> rowTypeFromData = dataProcess.getRowTypeFromData(typeFactory);
//
//    // TODO : see how to use the map directly
//    RelDataType dataType = typeFactory.createStructType(Pair.zip(
//        new ArrayList<String>(rowTypeFromData.keySet()),
//        new ArrayList<RelDataType>(rowTypeFromData.values())
//    ));
//    System.out.println("RedisearchTable.getRowType() dataType : " + dataType );
//
//    return dataType;


  }

  @Override public Enumerable<Object[]> scan(DataContext root, List<RexNode> filters) {
    System.out.println("RedisearchTable.scan() = "
        + "\n\t root " + root.toString()
        + "\n\t filters " + filters
        + "\n\t schema " + schema
    );


    String queryString = " * ";


    if (filters.size() != 0) {

      // TODO : extract schema info to generate proper string
      // analyze filter
      System.out.println(" === FILTER ===");
      System.out.println(filters.get(0));
      RexNode filter = filters.get(0);

      System.out
          .println("\n\t getKind " + filter.getKind() + "\n\t getType " + filter.getType() + "");

      if (filter.isA(SqlKind.EQUALS)) {
        final RexCall call = (RexCall) filter;
        RexNode left = call.getOperands().get(0);
        RexNode right = call.getOperands().get(1);

        String field = ((RexCall) left).operands.get(1).toString();
        String value = ((RexLiteral) right).getValue2().toString();

        if (field.startsWith("'")) {
          field.substring(1, field.length() - 1);
        }


        queryString = "@".concat(field).concat(":{").concat(value).concat("}");

      }
      System.out.println(" === /FILTER ===");
    }

    final String q = queryString;


    return new AbstractEnumerable<Object[]>() {
      @Override public Enumerator<Object[]> enumerator() {
        return new RedisearchEnumator(redisConfig, schema, tableName, indexName, q);
      }
    };
  }

  @Override public String toString() {
    return "RedisearchTable{" + "rowType=" + rowType + ", schema=" + schema + ", tableName='"
        + tableName + '\'' + ", indexName='" + indexName + '\'' + ", protoRowType=" + protoRowType
        + ", redisConfig=" + redisConfig + '}';
  }
}
