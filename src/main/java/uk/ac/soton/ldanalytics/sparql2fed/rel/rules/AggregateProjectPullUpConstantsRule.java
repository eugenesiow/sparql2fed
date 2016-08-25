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

import uk.ac.soton.ldanalytics.sparql2fed.plan.RelOptPredicateList;
import uk.ac.soton.ldanalytics.sparql2fed.plan.RelOptRule;
import uk.ac.soton.ldanalytics.sparql2fed.plan.RelOptRuleCall;
import uk.ac.soton.ldanalytics.sparql2fed.rel.RelNode;
import uk.ac.soton.ldanalytics.sparql2fed.rel.core.Aggregate;
import uk.ac.soton.ldanalytics.sparql2fed.rel.core.AggregateCall;
import uk.ac.soton.ldanalytics.sparql2fed.rel.core.RelFactories;
import uk.ac.soton.ldanalytics.sparql2fed.rel.logical.LogicalAggregate;
import uk.ac.soton.ldanalytics.sparql2fed.rel.logical.LogicalProject;
import uk.ac.soton.ldanalytics.sparql2fed.rel.metadata.RelMetadataQuery;
import uk.ac.soton.ldanalytics.sparql2fed.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import uk.ac.soton.ldanalytics.sparql2fed.util.ImmutableBitSet;
import uk.ac.soton.ldanalytics.sparql2fed.util.Pair;
import uk.ac.soton.ldanalytics.sparql2fed.util.Permutation;

import com.google.common.collect.ImmutableMap;

import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;

/**
 * Planner rule that removes constant keys from an
 * {@link uk.ac.soton.ldanalytics.sparql2fed.rel.core.Aggregate}.
 *
 * <p>Constant fields are deduced using
 * {@link RelMetadataQuery#getPulledUpPredicates(RelNode)}; the input does not
 * need to be a {@link uk.ac.soton.ldanalytics.sparql2fed.rel.core.Project}.
 *
 * <p>This rules never removes the last column, because {@code Aggregate([])}
 * returns 1 row even if its input is empty.
 *
 * <p>Since the transformed relational expression has to match the original
 * relational expression, the constants are placed in a projection above the
 * reduced aggregate. If those constants are not used, another rule will remove
 * them from the project.
 */
public class AggregateProjectPullUpConstantsRule extends RelOptRule {
  //~ Static fields/initializers ---------------------------------------------

  /** The singleton. */
  public static final AggregateProjectPullUpConstantsRule INSTANCE =
      new AggregateProjectPullUpConstantsRule(LogicalAggregate.class,
          LogicalProject.class, RelFactories.LOGICAL_BUILDER,
          "AggregateProjectPullUpConstantsRule");

  /** More general instance that matches any relational expression. */
  public static final AggregateProjectPullUpConstantsRule INSTANCE2 =
      new AggregateProjectPullUpConstantsRule(LogicalAggregate.class,
          RelNode.class, RelFactories.LOGICAL_BUILDER,
          "AggregatePullUpConstantsRule");

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates an AggregateProjectPullUpConstantsRule.
   *
   * @param aggregateClass Aggregate class
   * @param inputClass Input class, such as {@link LogicalProject}
   * @param relBuilderFactory Builder for relational expressions
   * @param description Description, or null to guess description
   */
  public AggregateProjectPullUpConstantsRule(
      Class<? extends Aggregate> aggregateClass,
      Class<? extends RelNode> inputClass,
      RelBuilderFactory relBuilderFactory, String description) {
    super(
        operand(aggregateClass, null, Aggregate.IS_SIMPLE,
            operand(inputClass, any())),
        relBuilderFactory, description);
  }

  //~ Methods ----------------------------------------------------------------

  public void onMatch(RelOptRuleCall call) {
    final Aggregate aggregate = call.rel(0);
    final RelNode input = call.rel(1);

    assert !aggregate.indicator : "predicate ensured no grouping sets";
    final int groupCount = aggregate.getGroupCount();
    if (groupCount == 1) {
      // No room for optimization since we cannot convert from non-empty
      // GROUP BY list to the empty one.
      return;
    }

    final RexBuilder rexBuilder = aggregate.getCluster().getRexBuilder();
    final RelMetadataQuery mq = RelMetadataQuery.instance();
    final RelOptPredicateList predicates =
        mq.getPulledUpPredicates(aggregate.getInput());
    if (predicates == null) {
      return;
    }
    final ImmutableMap<RexNode, RexNode> constants =
        ReduceExpressionsRule.predicateConstants(RexNode.class, rexBuilder,
            predicates);
    final NavigableMap<Integer, RexNode> map = new TreeMap<>();
    for (int key : aggregate.getGroupSet()) {
      final RexInputRef ref =
          rexBuilder.makeInputRef(aggregate.getInput(), key);
      if (constants.containsKey(ref)) {
        map.put(key, constants.get(ref));
      }
    }

    // None of the group expressions are constant. Nothing to do.
    if (map.isEmpty()) {
      return;
    }

    if (groupCount == map.size()) {
      // At least a single item in group by is required.
      // Otherwise "GROUP BY 1, 2" might be altered to "GROUP BY ()".
      // Removing of the first element is not optimal here,
      // however it will allow us to use fast path below (just trim
      // groupCount).
      map.remove(map.navigableKeySet().first());
    }

    ImmutableBitSet newGroupSet = aggregate.getGroupSet();
    for (int key : map.keySet()) {
      newGroupSet = newGroupSet.clear(key);
    }
    final int newGroupCount = newGroupSet.cardinality();

    // If the constants are on the trailing edge of the group list, we just
    // reduce the group count.
    final RelBuilder relBuilder = call.builder();
    relBuilder.push(input);
    if (map.navigableKeySet().first() == newGroupCount) {
      // Clone aggregate calls.
      final List<AggregateCall> newAggCalls = new ArrayList<>();
      for (AggregateCall aggCall : aggregate.getAggCallList()) {
        newAggCalls.add(
            aggCall.adaptTo(input, aggCall.getArgList(), aggCall.filterArg,
                groupCount, newGroupCount));
      }
      relBuilder.aggregate(
          relBuilder.groupKey(newGroupSet, false, null),
          newAggCalls);
    } else {
      // Create the mapping from old field positions to new field
      // positions.
      final Permutation mapping =
          new Permutation(input.getRowType().getFieldCount());
      mapping.identity();

      // Ensure that the first positions in the mapping are for the new
      // group columns.
      for (int i = 0, groupOrdinal = 0, constOrdinal = newGroupCount;
          i < groupCount;
          ++i) {
        if (map.containsKey(i)) {
          mapping.set(i, constOrdinal++);
        } else {
          mapping.set(i, groupOrdinal++);
        }
      }

      // Adjust aggregate calls for new field positions.
      final List<AggregateCall> newAggCalls = new ArrayList<>();
      for (AggregateCall aggCall : aggregate.getAggCallList()) {
        final int argCount = aggCall.getArgList().size();
        final List<Integer> args = new ArrayList<>(argCount);
        for (int j = 0; j < argCount; j++) {
          final Integer arg = aggCall.getArgList().get(j);
          args.add(mapping.getTarget(arg));
        }
        final int filterArg = aggCall.filterArg < 0 ? aggCall.filterArg
            : mapping.getTarget(aggCall.filterArg);
        newAggCalls.add(
            aggCall.adaptTo(relBuilder.peek(), args, filterArg, groupCount,
                newGroupCount));
      }

      // Aggregate on projection.
      relBuilder.aggregate(
          relBuilder.groupKey(newGroupSet, false, null),
              newAggCalls);
    }

    // Create a projection back again.
    List<Pair<RexNode, String>> projects = new ArrayList<>();
    int source = 0;
    for (RelDataTypeField field : aggregate.getRowType().getFieldList()) {
      RexNode expr;
      final int i = field.getIndex();
      if (i >= groupCount) {
        // Aggregate expressions' names and positions are unchanged.
        expr = relBuilder.field(i - map.size());
      } else if (map.containsKey(i)) {
        // Re-generate the constant expression in the project.
        expr = map.get(i);
      } else {
        // Project the aggregation expression, in its original
        // position.
        expr = relBuilder.field(source);
        ++source;
      }
      projects.add(Pair.of(expr, field.getName()));
    }
    relBuilder.project(Pair.left(projects), Pair.right(projects)); // inverse
    call.transformTo(relBuilder.build());
  }

}

// End AggregateProjectPullUpConstantsRule.java