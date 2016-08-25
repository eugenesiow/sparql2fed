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
package uk.ac.soton.ldanalytics.sparql2fed.rel.rules;

import org.apache.calcite.linq4j.Ord;
import uk.ac.soton.ldanalytics.sparql2fed.plan.RelOptRule;
import uk.ac.soton.ldanalytics.sparql2fed.plan.RelOptRuleCall;
import uk.ac.soton.ldanalytics.sparql2fed.rel.RelNode;
import uk.ac.soton.ldanalytics.sparql2fed.rel.core.Aggregate;
import uk.ac.soton.ldanalytics.sparql2fed.rel.core.AggregateCall;
import uk.ac.soton.ldanalytics.sparql2fed.rel.core.RelFactories;
import uk.ac.soton.ldanalytics.sparql2fed.rel.core.Union;
import uk.ac.soton.ldanalytics.sparql2fed.rel.logical.LogicalAggregate;
import uk.ac.soton.ldanalytics.sparql2fed.rel.logical.LogicalUnion;
import uk.ac.soton.ldanalytics.sparql2fed.rel.metadata.RelMdUtil;
import uk.ac.soton.ldanalytics.sparql2fed.rel.metadata.RelMetadataQuery;
import uk.ac.soton.ldanalytics.sparql2fed.rel.type.RelDataType;
import uk.ac.soton.ldanalytics.sparql2fed.sql.SqlAggFunction;
import uk.ac.soton.ldanalytics.sparql2fed.sql.fun.SqlCountAggFunction;
import uk.ac.soton.ldanalytics.sparql2fed.sql.fun.SqlMinMaxAggFunction;
import uk.ac.soton.ldanalytics.sparql2fed.sql.fun.SqlStdOperatorTable;
import uk.ac.soton.ldanalytics.sparql2fed.sql.fun.SqlSumAggFunction;
import uk.ac.soton.ldanalytics.sparql2fed.sql.fun.SqlSumEmptyIsZeroAggFunction;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

/**
 * Planner rule that pushes an
 * {@link uk.ac.soton.ldanalytics.sparql2fed.rel.core.Aggregate}
 * past a non-distinct {@link uk.ac.soton.ldanalytics.sparql2fed.rel.core.Union}.
 */
public class AggregateUnionTransposeRule extends RelOptRule {
  public static final AggregateUnionTransposeRule INSTANCE =
      new AggregateUnionTransposeRule(LogicalAggregate.class,
          LogicalUnion.class, RelFactories.LOGICAL_BUILDER);

  private static final Map<Class<? extends SqlAggFunction>, Boolean>
  SUPPORTED_AGGREGATES = new IdentityHashMap<>();

  static {
    SUPPORTED_AGGREGATES.put(SqlMinMaxAggFunction.class, true);
    SUPPORTED_AGGREGATES.put(SqlCountAggFunction.class, true);
    SUPPORTED_AGGREGATES.put(SqlSumAggFunction.class, true);
    SUPPORTED_AGGREGATES.put(SqlSumEmptyIsZeroAggFunction.class, true);
  }

  /** Creates an AggregateUnionTransposeRule. */
  public AggregateUnionTransposeRule(Class<? extends Aggregate> aggregateClass,
      Class<? extends Union> unionClass, RelBuilderFactory relBuilderFactory) {
    super(
        operand(aggregateClass,
            operand(unionClass, any())),
        relBuilderFactory, null);
  }

  @Deprecated // to be removed before 2.0
  public AggregateUnionTransposeRule(Class<? extends Aggregate> aggregateClass,
      RelFactories.AggregateFactory aggregateFactory,
      Class<? extends Union> unionClass,
      RelFactories.SetOpFactory setOpFactory) {
    this(aggregateClass, unionClass,
        RelBuilder.proto(aggregateFactory, setOpFactory));
  }

  public void onMatch(RelOptRuleCall call) {
    Aggregate aggRel = call.rel(0);
    Union union = call.rel(1);

    if (!union.all) {
      // This transformation is only valid for UNION ALL.
      // Consider t1(i) with rows (5), (5) and t2(i) with
      // rows (5), (10), and the query
      // select sum(i) from (select i from t1) union (select i from t2).
      // The correct answer is 15.  If we apply the transformation,
      // we get
      // select sum(i) from
      // (select sum(i) as i from t1) union (select sum(i) as i from t2)
      // which yields 25 (incorrect).
      return;
    }

    int groupCount = aggRel.getGroupSet().cardinality();

    List<AggregateCall> transformedAggCalls =
        transformAggCalls(
            aggRel.copy(aggRel.getTraitSet(), aggRel.getInput(), false,
                aggRel.getGroupSet(), null, aggRel.getAggCallList()),
            groupCount, aggRel.getAggCallList());
    if (transformedAggCalls == null) {
      // we've detected the presence of something like AVG,
      // which we can't handle
      return;
    }

    // create corresponding aggregates on top of each union child
    final RelBuilder relBuilder = call.builder();
    int transformCount = 0;
    final RelMetadataQuery mq = RelMetadataQuery.instance();
    for (RelNode input : union.getInputs()) {
      boolean alreadyUnique =
          RelMdUtil.areColumnsDefinitelyUnique(mq, input,
              aggRel.getGroupSet());

      relBuilder.push(input);
      if (!alreadyUnique) {
        ++transformCount;
        relBuilder.aggregate(relBuilder.groupKey(aggRel.getGroupSet(), false, null),
            aggRel.getAggCallList());
      }
    }

    if (transformCount == 0) {
      // none of the children could benefit from the push-down,
      // so bail out (preventing the infinite loop to which most
      // planners would succumb)
      return;
    }

    // create a new union whose children are the aggregates created above
    relBuilder.union(true, union.getInputs().size());
    relBuilder.aggregate(
        relBuilder.groupKey(aggRel.getGroupSet(), aggRel.indicator, aggRel.getGroupSets()),
        transformedAggCalls);
    call.transformTo(relBuilder.build());
  }

  private List<AggregateCall> transformAggCalls(RelNode input, int groupCount,
      List<AggregateCall> origCalls) {
    final List<AggregateCall> newCalls = Lists.newArrayList();
    for (Ord<AggregateCall> ord : Ord.zip(origCalls)) {
      final AggregateCall origCall = ord.e;
      if (origCall.isDistinct()
          || !SUPPORTED_AGGREGATES.containsKey(origCall.getAggregation()
              .getClass())) {
        return null;
      }
      final SqlAggFunction aggFun;
      final RelDataType aggType;
      if (origCall.getAggregation() == SqlStdOperatorTable.COUNT) {
        aggFun = SqlStdOperatorTable.SUM0;
        // count(any) is always not null, however nullability of sum might
        // depend on the number of columns in GROUP BY.
        // Here we use SUM0 since we are sure we will not face nullable
        // inputs nor we'll face empty set.
        aggType = null;
      } else {
        aggFun = origCall.getAggregation();
        aggType = origCall.getType();
      }
      AggregateCall newCall =
          AggregateCall.create(aggFun, origCall.isDistinct(),
              ImmutableList.of(groupCount + ord.i), -1, groupCount, input,
              aggType, origCall.getName());
      newCalls.add(newCall);
    }
    return newCalls;
  }
}

// End AggregateUnionTransposeRule.java
