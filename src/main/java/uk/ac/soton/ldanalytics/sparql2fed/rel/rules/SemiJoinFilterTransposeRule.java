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
import uk.ac.soton.ldanalytics.sparql2fed.rel.core.SemiJoin;
import uk.ac.soton.ldanalytics.sparql2fed.rel.logical.LogicalFilter;

/**
 * Planner rule that pushes
 * {@link uk.ac.soton.ldanalytics.sparql2fed.rel.core.SemiJoin}s down in a tree past
 * a {@link uk.ac.soton.ldanalytics.sparql2fed.rel.core.Filter}.
 *
 * <p>The intention is to trigger other rules that will convert
 * {@code SemiJoin}s.
 *
 * <p>SemiJoin(LogicalFilter(X), Y) &rarr; LogicalFilter(SemiJoin(X, Y))
 *
 * @see SemiJoinProjectTransposeRule
 */
public class SemiJoinFilterTransposeRule extends RelOptRule {
  public static final SemiJoinFilterTransposeRule INSTANCE =
      new SemiJoinFilterTransposeRule();

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a SemiJoinFilterTransposeRule.
   */
  private SemiJoinFilterTransposeRule() {
    super(
        operand(SemiJoin.class,
            some(operand(LogicalFilter.class, any()))));
  }

  //~ Methods ----------------------------------------------------------------

  // implement RelOptRule
  public void onMatch(RelOptRuleCall call) {
    SemiJoin semiJoin = call.rel(0);
    LogicalFilter filter = call.rel(1);

    RelNode newSemiJoin =
        SemiJoin.create(filter.getInput(),
            semiJoin.getRight(),
            semiJoin.getCondition(),
            semiJoin.getLeftKeys(),
            semiJoin.getRightKeys());

    RelNode newFilter =
        RelOptUtil.createFilter(
            newSemiJoin,
            filter.getCondition());

    call.transformTo(newFilter);
  }
}

// End SemiJoinFilterTransposeRule.java
