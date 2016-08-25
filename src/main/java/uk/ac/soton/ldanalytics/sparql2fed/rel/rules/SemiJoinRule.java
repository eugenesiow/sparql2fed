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

import uk.ac.soton.ldanalytics.sparql2fed.plan.RelOptCluster;
import uk.ac.soton.ldanalytics.sparql2fed.plan.RelOptRule;
import uk.ac.soton.ldanalytics.sparql2fed.plan.RelOptRuleCall;
import uk.ac.soton.ldanalytics.sparql2fed.plan.RelOptUtil;
import uk.ac.soton.ldanalytics.sparql2fed.rel.RelNode;
import uk.ac.soton.ldanalytics.sparql2fed.rel.core.Aggregate;
import uk.ac.soton.ldanalytics.sparql2fed.rel.core.Join;
import uk.ac.soton.ldanalytics.sparql2fed.rel.core.JoinInfo;
import uk.ac.soton.ldanalytics.sparql2fed.rel.core.Project;
import uk.ac.soton.ldanalytics.sparql2fed.rel.core.SemiJoin;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import uk.ac.soton.ldanalytics.sparql2fed.util.ImmutableBitSet;
import uk.ac.soton.ldanalytics.sparql2fed.util.ImmutableIntList;

import com.google.common.collect.Lists;

import java.util.List;

/**
 * Planner rule that creates a {@code SemiJoinRule} from a
 * {@link uk.ac.soton.ldanalytics.sparql2fed.rel.core.Join} on top of a
 * {@link uk.ac.soton.ldanalytics.sparql2fed.rel.logical.LogicalAggregate}.
 */
public class SemiJoinRule extends RelOptRule {
  public static final SemiJoinRule INSTANCE = new SemiJoinRule();

  private SemiJoinRule() {
    super(
        operand(Project.class,
            some(
                operand(Join.class,
                    some(operand(RelNode.class, any()),
                        operand(Aggregate.class, any()))))));
  }

  @Override public void onMatch(RelOptRuleCall call) {
    final Project project = call.rel(0);
    final Join join = call.rel(1);
    final RelNode left = call.rel(2);
    final Aggregate aggregate = call.rel(3);
    final RelOptCluster cluster = join.getCluster();
    final RexBuilder rexBuilder = cluster.getRexBuilder();
    final ImmutableBitSet bits =
        RelOptUtil.InputFinder.bits(project.getProjects(), null);
    final ImmutableBitSet rightBits =
        ImmutableBitSet.range(left.getRowType().getFieldCount(),
            join.getRowType().getFieldCount());
    if (bits.intersects(rightBits)) {
      return;
    }
    final JoinInfo joinInfo = join.analyzeCondition();
    if (!joinInfo.rightSet().equals(
        ImmutableBitSet.range(aggregate.getGroupCount()))) {
      // Rule requires that aggregate key to be the same as the join key.
      // By the way, neither a super-set nor a sub-set would work.
      return;
    }
    if (!joinInfo.isEqui()) {
      return;
    }
    final List<Integer> newRightKeyBuilder = Lists.newArrayList();
    final List<Integer> aggregateKeys = aggregate.getGroupSet().asList();
    for (int key : joinInfo.rightKeys) {
      newRightKeyBuilder.add(aggregateKeys.get(key));
    }
    final ImmutableIntList newRightKeys =
        ImmutableIntList.copyOf(newRightKeyBuilder);
    final RelNode newRight = aggregate.getInput();
    final RexNode newCondition =
        RelOptUtil.createEquiJoinCondition(left, joinInfo.leftKeys, newRight,
            newRightKeys, rexBuilder);
    final SemiJoin semiJoin =
        SemiJoin.create(left, newRight, newCondition, joinInfo.leftKeys,
            newRightKeys);
    final Project newProject =
        project.copy(project.getTraitSet(), semiJoin, project.getProjects(),
            project.getRowType());
    call.transformTo(ProjectRemoveRule.strip(newProject));
  }
}

// End SemiJoinRule.java
