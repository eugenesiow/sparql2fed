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
import uk.ac.soton.ldanalytics.sparql2fed.rel.core.SetOp;
import uk.ac.soton.ldanalytics.sparql2fed.rel.logical.LogicalProject;
import org.apache.calcite.rex.RexInputRef;

import java.util.ArrayList;
import java.util.List;

/**
 * Planner rule that pushes
 * a {@link uk.ac.soton.ldanalytics.sparql2fed.rel.logical.LogicalProject}
 * past a {@link uk.ac.soton.ldanalytics.sparql2fed.rel.core.SetOp}.
 *
 * <p>The children of the {@code SetOp} will project
 * only the {@link RexInputRef}s referenced in the original
 * {@code LogicalProject}.
 */
public class ProjectSetOpTransposeRule extends RelOptRule {
  public static final ProjectSetOpTransposeRule INSTANCE =
      new ProjectSetOpTransposeRule(PushProjector.ExprCondition.FALSE);

  //~ Instance fields --------------------------------------------------------

  /**
   * Expressions that should be preserved in the projection
   */
  private PushProjector.ExprCondition preserveExprCondition;

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a ProjectSetOpTransposeRule with an explicit condition whether
   * to preserve expressions.
   *
   * @param preserveExprCondition Condition whether to preserve expressions
   */
  public ProjectSetOpTransposeRule(
      PushProjector.ExprCondition preserveExprCondition) {
    super(
        operand(
            LogicalProject.class,
            operand(SetOp.class, any())));
    this.preserveExprCondition = preserveExprCondition;
  }

  //~ Methods ----------------------------------------------------------------

  // implement RelOptRule
  public void onMatch(RelOptRuleCall call) {
    LogicalProject origProj = call.rel(0);
    SetOp setOp = call.rel(1);

    // cannot push project past a distinct
    if (!setOp.all) {
      return;
    }

    // locate all fields referenced in the projection
    PushProjector pushProject =
        new PushProjector(origProj, null, setOp, preserveExprCondition);
    pushProject.locateAllRefs();

    List<RelNode> newSetOpInputs = new ArrayList<RelNode>();
    int[] adjustments = pushProject.getAdjustments();

    // push the projects completely below the setop; this
    // is different from pushing below a join, where we decompose
    // to try to keep expensive expressions above the join,
    // because UNION ALL does not have any filtering effect,
    // and it is the only operator this rule currently acts on
    for (RelNode input : setOp.getInputs()) {
      // be lazy:  produce two ProjectRels, and let another rule
      // merge them (could probably just clone origProj instead?)
      LogicalProject p =
          pushProject.createProjectRefsAndExprs(
              input, true, false);
      newSetOpInputs.add(
          pushProject.createNewProject(p, adjustments));
    }

    // create a new setop whose children are the ProjectRels created above
    SetOp newSetOp =
        setOp.copy(setOp.getTraitSet(), newSetOpInputs);

    call.transformTo(newSetOp);
  }
}

// End ProjectSetOpTransposeRule.java
