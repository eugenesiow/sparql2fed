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

import uk.ac.soton.ldanalytics.sparql2fed.plan.RelOptRule;
import uk.ac.soton.ldanalytics.sparql2fed.plan.RelOptRuleCall;
import uk.ac.soton.ldanalytics.sparql2fed.rel.RelNode;
import uk.ac.soton.ldanalytics.sparql2fed.rel.core.Aggregate;
import uk.ac.soton.ldanalytics.sparql2fed.rel.core.RelFactories;
import uk.ac.soton.ldanalytics.sparql2fed.rel.core.Union;
import uk.ac.soton.ldanalytics.sparql2fed.rel.logical.LogicalAggregate;
import uk.ac.soton.ldanalytics.sparql2fed.rel.logical.LogicalUnion;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;

/**
 * Planner rule that matches
 * {@link uk.ac.soton.ldanalytics.sparql2fed.rel.core.Aggregate}s beneath a
 * {@link uk.ac.soton.ldanalytics.sparql2fed.rel.core.Union} and pulls them up, so
 * that a single
 * {@link uk.ac.soton.ldanalytics.sparql2fed.rel.core.Aggregate} removes duplicates.
 *
 * <p>This rule only handles cases where the
 * {@link uk.ac.soton.ldanalytics.sparql2fed.rel.core.Union}s
 * still have only two inputs.
 */
public class AggregateUnionAggregateRule extends RelOptRule {
  public static final AggregateUnionAggregateRule INSTANCE =
      new AggregateUnionAggregateRule(LogicalAggregate.class,
          LogicalUnion.class, RelFactories.LOGICAL_BUILDER);

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a AggregateUnionAggregateRule.
   */
  public AggregateUnionAggregateRule(Class<? extends Aggregate> aggregateClass,
      Class<? extends Union> unionClass, RelBuilderFactory relBuilderFactory) {
    super(
        operand(aggregateClass, null, Aggregate.IS_SIMPLE,
            operand(unionClass,
                operand(RelNode.class, any()),
                operand(RelNode.class, any()))),
        relBuilderFactory, null);
  }

  @Deprecated // to be removed before 2.0
  public AggregateUnionAggregateRule(Class<? extends Aggregate> aggregateClass,
      RelFactories.AggregateFactory aggregateFactory,
      Class<? extends Union> unionClass,
      RelFactories.SetOpFactory setOpFactory) {
    this(aggregateClass, unionClass,
        RelBuilder.proto(aggregateFactory, setOpFactory));
  }

  //~ Methods ----------------------------------------------------------------

  public void onMatch(RelOptRuleCall call) {
    final Aggregate topAggRel = call.rel(0);
    final Union union = call.rel(1);

    // If distincts haven't been removed yet, defer invoking this rule
    if (!union.all) {
      return;
    }

    // We want to apply this rule on the pattern where the LogicalAggregate
    // is the second input into the Union first.  Hence, that's why the
    // rule pattern matches on generic RelNodes rather than explicit
    // UnionRels.  By doing so, and firing this rule in a bottom-up order,
    // it allows us to only specify a single pattern for this rule.
    final RelBuilder relBuilder = call.builder();
    final Aggregate bottomAggRel;
    if (call.rel(3) instanceof Aggregate) {
      bottomAggRel = call.rel(3);
      relBuilder.push(call.rel(2))
          .push(call.rel(3).getInput(0));
    } else if (call.rel(2) instanceof Aggregate) {
      bottomAggRel = call.rel(2);
      relBuilder.push(call.rel(2).getInput(0))
          .push(call.rel(3));
    } else {
      return;
    }

    // Only pull up aggregates if they are there just to remove distincts
    if (!topAggRel.getAggCallList().isEmpty()
        || !bottomAggRel.getAggCallList().isEmpty()) {
      return;
    }

    relBuilder.union(true);
    relBuilder.aggregate(relBuilder.groupKey(topAggRel.getGroupSet(), false, null),
        topAggRel.getAggCallList());
    call.transformTo(relBuilder.build());
  }
}

// End AggregateUnionAggregateRule.java
