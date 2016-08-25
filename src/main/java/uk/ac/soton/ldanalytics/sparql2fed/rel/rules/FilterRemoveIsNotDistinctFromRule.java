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
import uk.ac.soton.ldanalytics.sparql2fed.plan.RelOptUtil;
import uk.ac.soton.ldanalytics.sparql2fed.rel.RelNode;
import uk.ac.soton.ldanalytics.sparql2fed.rel.logical.LogicalFilter;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexUtil;
import uk.ac.soton.ldanalytics.sparql2fed.sql.fun.SqlStdOperatorTable;

/**
 * Planner rule that replaces {@code IS NOT DISTINCT FROM}
 * in a {@link uk.ac.soton.ldanalytics.sparql2fed.rel.logical.LogicalFilter}
 * with logically equivalent operations.
 *
 * @see uk.ac.soton.ldanalytics.sparql2fed.sql.fun.SqlStdOperatorTable#IS_NOT_DISTINCT_FROM
 */
public final class FilterRemoveIsNotDistinctFromRule extends RelOptRule {
  //~ Static fields/initializers ---------------------------------------------

  /** The singleton. */
  public static final FilterRemoveIsNotDistinctFromRule INSTANCE =
      new FilterRemoveIsNotDistinctFromRule();

  //~ Constructors -----------------------------------------------------------

  private FilterRemoveIsNotDistinctFromRule() {
    super(operand(LogicalFilter.class, any()));
  }

  //~ Methods ----------------------------------------------------------------

  public void onMatch(RelOptRuleCall call) {
    LogicalFilter oldFilter = call.rel(0);
    RexNode oldFilterCond = oldFilter.getCondition();

    if (RexUtil.findOperatorCall(
        SqlStdOperatorTable.IS_NOT_DISTINCT_FROM,
        oldFilterCond)
        == null) {
      // no longer contains isNotDistinctFromOperator
      return;
    }

    // Now replace all the "a isNotDistinctFrom b"
    // with the RexNode given by RelOptUtil.isDistinctFrom() method

    RemoveIsNotDistinctFromRexShuttle rewriteShuttle =
        new RemoveIsNotDistinctFromRexShuttle(
            oldFilter.getCluster().getRexBuilder());

    RelNode newFilterRel =
        RelOptUtil.createFilter(
            oldFilter.getInput(),
            oldFilterCond.accept(rewriteShuttle));

    call.transformTo(newFilterRel);
  }

  //~ Inner Classes ----------------------------------------------------------

  /** Shuttle that removes 'x IS NOT DISTINCT FROM y' and converts it
   * to 'CASE WHEN x IS NULL THEN y IS NULL WHEN y IS NULL THEN x IS
   * NULL ELSE x = y END'. */
  private class RemoveIsNotDistinctFromRexShuttle extends RexShuttle {
    RexBuilder rexBuilder;

    public RemoveIsNotDistinctFromRexShuttle(
        RexBuilder rexBuilder) {
      this.rexBuilder = rexBuilder;
    }

    // override RexShuttle
    public RexNode visitCall(RexCall call) {
      RexNode newCall = super.visitCall(call);

      if (call.getOperator()
          == SqlStdOperatorTable.IS_NOT_DISTINCT_FROM) {
        RexCall tmpCall = (RexCall) newCall;
        newCall =
            RelOptUtil.isDistinctFrom(
                rexBuilder,
                tmpCall.operands.get(0),
                tmpCall.operands.get(1),
                true);
      }
      return newCall;
    }
  }
}

// End FilterRemoveIsNotDistinctFromRule.java
