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
import uk.ac.soton.ldanalytics.sparql2fed.rel.RelCollation;
import uk.ac.soton.ldanalytics.sparql2fed.rel.RelCollationTraitDef;
import uk.ac.soton.ldanalytics.sparql2fed.rel.RelFieldCollation;
import uk.ac.soton.ldanalytics.sparql2fed.rel.RelNode;
import uk.ac.soton.ldanalytics.sparql2fed.rel.core.Project;
import uk.ac.soton.ldanalytics.sparql2fed.rel.core.Sort;
import uk.ac.soton.ldanalytics.sparql2fed.rel.logical.LogicalProject;
import org.apache.calcite.rex.RexUtil;
import uk.ac.soton.ldanalytics.sparql2fed.util.mapping.Mappings;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.Map;

/**
 * Planner rule that pushes
 * a {@link uk.ac.soton.ldanalytics.sparql2fed.rel.core.Sort}
 * past a {@link uk.ac.soton.ldanalytics.sparql2fed.rel.core.Project}.
 *
 * @see uk.ac.soton.ldanalytics.sparql2fed.rel.rules.ProjectSortTransposeRule
 */
public class SortProjectTransposeRule extends RelOptRule {
  public static final SortProjectTransposeRule INSTANCE =
      new SortProjectTransposeRule(Sort.class, LogicalProject.class, null);

  //~ Constructors -----------------------------------------------------------

  @Deprecated // to be removed before 2.0
  public SortProjectTransposeRule(
      Class<? extends Sort> sortClass,
      Class<? extends Project> projectClass) {
    this(sortClass, projectClass, null);
  }

  /** Creates a SortProjectTransposeRule.*/
  public SortProjectTransposeRule(
      Class<? extends Sort> sortClass,
      Class<? extends Project> projectClass,
      String description) {
    super(
        operand(sortClass,
            operand(projectClass, any())),
        description);
  }

  //~ Methods ----------------------------------------------------------------

  public void onMatch(RelOptRuleCall call) {
    final Sort sort = call.rel(0);
    final Project project = call.rel(1);
    final RelOptCluster cluster = project.getCluster();

    if (sort.getConvention() != project.getConvention()) {
      return;
    }

    // Determine mapping between project input and output fields. If sort
    // relies on non-trivial expressions, we can't push.
    final Mappings.TargetMapping map =
        RelOptUtil.permutation(
            project.getProjects(), project.getInput().getRowType());
    for (RelFieldCollation fc : sort.getCollation().getFieldCollations()) {
      if (map.getTargetOpt(fc.getFieldIndex()) < 0) {
        return;
      }
    }
    final RelCollation newCollation =
        cluster.traitSet().canonize(
            RexUtil.apply(map, sort.getCollation()));
    final Sort newSort =
        sort.copy(
            sort.getTraitSet().replace(newCollation),
            project.getInput(),
            newCollation,
            sort.offset,
            sort.fetch);
    RelNode newProject =
        project.copy(
            sort.getTraitSet(),
            ImmutableList.<RelNode>of(newSort));
    // Not only is newProject equivalent to sort;
    // newSort is equivalent to project's input
    // (but only if the sort is not also applying an offset/limit).
    Map<RelNode, RelNode> equiv;
    if (sort.offset == null
        && sort.fetch == null
        && cluster.getPlanner().getRelTraitDefs()
            .contains(RelCollationTraitDef.INSTANCE)) {
      equiv = ImmutableMap.of((RelNode) newSort, project.getInput());
    } else {
      equiv = ImmutableMap.of();
    }
    call.transformTo(newProject, equiv);
  }
}

// End SortProjectTransposeRule.java
