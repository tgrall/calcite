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

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.util.trace.CalciteTrace;

import org.slf4j.Logger;

public class RediSearchRules {
  private RediSearchRules() {}

  protected static final Logger LOGGER = CalciteTrace.getPlannerTracer();


  public static final RelOptRule[] RULES = {
      RedisearchSortRule.INSTANCE
  };


  /** Base class for planner rules that convert a relational expression to
   * RediSearch calling convention. */
  abstract static class RediSearchConverterRule extends ConverterRule {
    protected RediSearchConverterRule(Config config) {
      super(config);
    }
  }


  private static class RedisearchSortRule extends RediSearchConverterRule {
    static final RedisearchSortRule INSTANCE = ConverterRule.Config.INSTANCE
        .withConversion(Sort.class, Convention.NONE, RedisearchRel.CONVENTION,
            "RedisearchSortRule")
        .withRuleFactory(RedisearchSortRule::new)
        .toRule(RedisearchSortRule.class);

    RedisearchSortRule(ConverterRule.Config config) {
      super(config);
    }

    @Override public RelNode convert(RelNode rel) {
      final Sort sort = (Sort) rel;
      final RelTraitSet traitSet =
          sort.getTraitSet().replace(out)
              .replace(sort.getCollation());
      return new RedisearchSort(rel.getCluster(), traitSet,
          convert(sort.getInput(), traitSet.replace(RelCollations.EMPTY)),
          sort.getCollation(), sort.offset, sort.fetch);
    }
  }




  }
