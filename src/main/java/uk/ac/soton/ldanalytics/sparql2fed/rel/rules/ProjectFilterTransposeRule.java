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
import uk.ac.soton.ldanalytics.sparql2fed.rel.core.Filter;
import uk.ac.soton.ldanalytics.sparql2fed.rel.core.Project;
import uk.ac.soton.ldanalytics.sparql2fed.rel.core.RelFactories;
import uk.ac.soton.ldanalytics.sparql2fed.rel.logical.LogicalFilter;
import uk.ac.soton.ldanalytics.sparql2fed.rel.logical.LogicalProject;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.tools.RelBuilderFactory;

/**
 * Planner rule that pushes a {@link uk.ac.soton.ldanalytics.sparql2fed.rel.core.Project}
 * past a {@link uk.ac.soton.ldanalytics.sparql2fed.rel.core.Filter}.
 */
public class ProjectFilterTransposeRule extends RelOptRule {
  public static final ProjectFilterTransposeRule INSTANCE = new ProjectFilterTransposeRule(
      LogicalProject.class, LogicalFilter.class, RelFactories.LOGICAL_BUILDER,
      PushProjector.ExprCondition.FALSE);

  //~ Instance fields --------------------------------------------------------

  /**
   * Expressions that should be preserved in the projection
   */
  private final PushProjector.ExprCondition preserveExprCondition;

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a ProjectFilterTransposeRule.
   *
   * @param preserveExprCondition Condition for expressions that should be
   *                              preserved in the projection
   */
  public ProjectFilterTransposeRule(
      Class<? extends Project> projectClass,
      Class<? extends Filter> filterClass,
      RelBuilderFactory relBuilderFactory,
      PushProjector.ExprCondition preserveExprCondition) {
    super(
        operand(
            projectClass,
            operand(filterClass, any())),
        relBuilderFactory, null);
    this.preserveExprCondition = preserveExprCondition;
  }

  //~ Methods ----------------------------------------------------------------

  // implement RelOptRule
  public void onMatch(RelOptRuleCall call) {
    LogicalProject origProj;
    LogicalFilter filter;

    if (call.rels.length == 2) {
      origProj = call.rel(0);
      filter = call.rel(1);
    } else {
      origProj = null;
      filter = call.rel(0);
    }
    RelNode rel = filter.getInput();
    RexNode origFilter = filter.getCondition();

    if ((origProj != null)
        && RexOver.containsOver(origProj.getProjects(), null)) {
      // Cannot push project through filter if project contains a windowed
      // aggregate -- it will affect row counts. Abort this rule
      // invocation; pushdown will be considered after the windowed
      // aggregate has been implemented. It's OK if the filter contains a
      // windowed aggregate.
      return;
    }

    PushProjector pushProjector =
        new PushProjector(
            origProj, origFilter, rel, preserveExprCondition);
    RelNode topProject = pushProjector.convertProject(null);

    if (topProject != null) {
      call.transformTo(topProject);
    }
  }
}

// End ProjectFilterTransposeRule.java