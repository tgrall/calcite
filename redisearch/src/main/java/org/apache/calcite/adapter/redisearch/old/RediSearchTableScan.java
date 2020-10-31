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

import com.google.common.collect.ImmutableList;

import org.apache.calcite.plan.*;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;

import java.util.List;

public class RediSearchTableScan extends TableScan implements RedisearchRel {
  final RedisearchTable redisearchTable;
  final RelDataType projectRowType;



  protected RediSearchTableScan(RelOptCluster cluster, RelTraitSet traitSet,
      RelOptTable table, RedisearchTable redisearchTable, RelDataType projectRowType) {
    super(cluster, traitSet, ImmutableList.of(), table);
    System.out.println("RediSearchTableScan.RediSearchTableScan()");
    this.redisearchTable = redisearchTable;
    this.projectRowType = projectRowType;

    assert redisearchTable != null;
    assert getConvention() == RedisearchRel.CONVENTION;
  }

  @Override public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    System.out.println("RediSearchTableScan.copy()");

    assert inputs.isEmpty();
    return this;
  }

  @Override public RelDataType deriveRowType() {
    System.out.println("RediSearchTableScan.deriveRowType() "+ projectRowType);

    return projectRowType != null ? projectRowType : super.deriveRowType();
  }

  @Override public RelOptCost computeSelfCost(RelOptPlanner planner,
      RelMetadataQuery mq) {
    System.out.println("RediSearchTableScan.computeSelfCost()");

    // scans with a small project list are cheaper
    final float f = projectRowType == null ? 1f
        : (float) projectRowType.getFieldCount() / 100f;
    return super.computeSelfCost(planner, mq).multiplyBy(.1 * f);
  }

  @Override public void register(RelOptPlanner planner) {
    System.out.println("RediSearchTableScan.register()");

    planner.addRule(RedisearchToEnumerableConverterRule.INSTANCE);
    for (RelOptRule rule : RediSearchRules.RULES) {
      planner.addRule(rule);
    }
  }

  @Override public void implement(Implementor implementor) {
    System.out.println("RediSearchTableScan.implement()");

    implementor.redisearchTable = redisearchTable;
    implementor.table = table;
  }

}
