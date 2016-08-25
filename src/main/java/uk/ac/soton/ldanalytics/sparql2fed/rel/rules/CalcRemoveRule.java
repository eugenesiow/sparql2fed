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
import uk.ac.soton.ldanalytics.sparql2fed.rel.logical.LogicalCalc;
import org.apache.calcite.rex.RexProgram;

/**
 * Planner rule that removes a trivial
 * {@link uk.ac.soton.ldanalytics.sparql2fed.rel.logical.LogicalCalc}.
 *
 * <p>A {@link uk.ac.soton.ldanalytics.sparql2fed.rel.logical.LogicalCalc}
 * is trivial if it projects its input fields in their
 * original order, and it does not filter.
 *
 * @see ProjectRemoveRule
 */
public class CalcRemoveRule extends RelOptRule {
  //~ Static fields/initializers ---------------------------------------------

  public static final CalcRemoveRule INSTANCE =
      new CalcRemoveRule();

  //~ Constructors -----------------------------------------------------------

  private CalcRemoveRule() {
    super(operand(LogicalCalc.class, any()));
  }

  //~ Methods ----------------------------------------------------------------

  public void onMatch(RelOptRuleCall call) {
    LogicalCalc calc = call.rel(0);
    RexProgram program = calc.getProgram();
    if (!program.isTrivial()) {
      return;
    }
    RelNode input = calc.getInput();
    input = call.getPlanner().register(input, calc);
    call.transformTo(
        convert(
            input,
            calc.getTraitSet()));
  }
}

// End CalcRemoveRule.java
