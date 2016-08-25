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
import uk.ac.soton.ldanalytics.sparql2fed.rel.logical.LogicalProject;
import org.apache.calcite.rex.RexProgram;

/**
 * Rule to convert a
 * {@link uk.ac.soton.ldanalytics.sparql2fed.rel.logical.LogicalProject} to a
 * {@link uk.ac.soton.ldanalytics.sparql2fed.rel.logical.LogicalCalc}
 *
 * <p>The rule does not fire if the child is a
 * {@link uk.ac.soton.ldanalytics.sparql2fed.rel.logical.LogicalProject},
 * {@link uk.ac.soton.ldanalytics.sparql2fed.rel.logical.LogicalFilter} or
 * {@link uk.ac.soton.ldanalytics.sparql2fed.rel.logical.LogicalCalc}. If it did, then the same
 * {@link uk.ac.soton.ldanalytics.sparql2fed.rel.logical.LogicalCalc} would be formed via
 * several transformation paths, which is a waste of effort.</p>
 *
 * @see FilterToCalcRule
 */
public class ProjectToCalcRule extends RelOptRule {
  //~ Static fields/initializers ---------------------------------------------

  public static final ProjectToCalcRule INSTANCE = new ProjectToCalcRule();

  //~ Constructors -----------------------------------------------------------

  private ProjectToCalcRule() {
    super(operand(LogicalProject.class, any()));
  }

  //~ Methods ----------------------------------------------------------------

  public void onMatch(RelOptRuleCall call) {
    final LogicalProject project = call.rel(0);
    final RelNode input = project.getInput();
    final RexProgram program =
        RexProgram.create(
            input.getRowType(),
            project.getProjects(),
            null,
            project.getRowType(),
            project.getCluster().getRexBuilder());
    final LogicalCalc calc = LogicalCalc.create(input, program);
    call.transformTo(calc);
  }
}

// End ProjectToCalcRule.java
