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

import org.apache.calcite.adapter.enumerable.*;
import org.apache.calcite.config.CalciteSystemProperty;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.plan.*;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterImpl;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.runtime.Hook;
import org.apache.calcite.util.Pair;

import java.util.List;

public class RedisearchToEnumerableConverter extends ConverterImpl
    implements EnumerableRel {

  protected RedisearchToEnumerableConverter(
      RelOptCluster cluster,
      RelTraitSet traits,
      RelNode input) {
    super(cluster, ConventionTraitDef.INSTANCE, traits, input);
    System.out.println( "RedisearchToEnumerableConverter.RedisearchToEnumerableConverter()" );

  }


  @Override public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    System.out.println( "RedisearchToEnumerableConverter.copy()" );

    return new RedisearchToEnumerableConverter(
        getCluster(), traitSet, sole(inputs));
  }

  @Override public RelOptCost computeSelfCost(RelOptPlanner planner,
      RelMetadataQuery mq) {
    System.out.println( "RedisearchToEnumerableConverter.computeSelfCost()" );

    return super.computeSelfCost(planner, mq).multiplyBy(.1);
  }


  @Override public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
    System.out.println( "RedisearchToEnumerableConverter.implement()" );
    final BlockBuilder list = new BlockBuilder();
    final RedisearchRel.Implementor redisearchImplementor =
        new RedisearchRel.Implementor(getCluster().getRexBuilder());
    redisearchImplementor.visitChild(0, getInput());
    final RelDataType rowType = getRowType();
    final PhysType physType =
        PhysTypeImpl.of(
            implementor.getTypeFactory(), rowType,
            pref.prefer(JavaRowFormat.ARRAY));

    List<String> opList = Pair.right(redisearchImplementor.list);

    if (CalciteSystemProperty.DEBUG.value()) {
      System.out.println("RediSearch: " + opList);
    }
    System.out.println("RediSearch => opList : "+ opList); // TODO : TUG
    Hook.QUERY_PLAN.run(opList);
    System.out.println("RediSearch => list : "+ list); // TODO : TUG


    return implementor.result(physType, list.toBlock());


  }

}
