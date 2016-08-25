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

import uk.ac.soton.ldanalytics.sparql2fed.plan.MaterializedViewSubstitutionVisitor;
import uk.ac.soton.ldanalytics.sparql2fed.plan.RelOptMaterialization;
import uk.ac.soton.ldanalytics.sparql2fed.plan.RelOptPlanner;
import uk.ac.soton.ldanalytics.sparql2fed.plan.RelOptRule;
import uk.ac.soton.ldanalytics.sparql2fed.plan.RelOptRuleCall;
import uk.ac.soton.ldanalytics.sparql2fed.plan.RelOptUtil;
import uk.ac.soton.ldanalytics.sparql2fed.plan.hep.HepPlanner;
import uk.ac.soton.ldanalytics.sparql2fed.plan.hep.HepProgram;
import uk.ac.soton.ldanalytics.sparql2fed.plan.hep.HepProgramBuilder;
import uk.ac.soton.ldanalytics.sparql2fed.plan.volcano.VolcanoPlanner;
import uk.ac.soton.ldanalytics.sparql2fed.rel.RelNode;
import uk.ac.soton.ldanalytics.sparql2fed.rel.core.Filter;
import uk.ac.soton.ldanalytics.sparql2fed.rel.core.RelFactories;
import uk.ac.soton.ldanalytics.sparql2fed.rel.core.TableScan;
import org.apache.calcite.tools.RelBuilderFactory;

import com.google.common.collect.ImmutableList;

import java.util.Collections;
import java.util.List;

/**
 * Planner rule that converts
 * a {@link uk.ac.soton.ldanalytics.sparql2fed.rel.core.Filter}
 * on a {@link uk.ac.soton.ldanalytics.sparql2fed.rel.core.TableScan}
 * to a {@link uk.ac.soton.ldanalytics.sparql2fed.rel.core.Filter} on Materialized View
 */
public class MaterializedViewFilterScanRule extends RelOptRule {
  public static final MaterializedViewFilterScanRule INSTANCE =
      new MaterializedViewFilterScanRule(RelFactories.LOGICAL_BUILDER);

  private final HepProgram program = new HepProgramBuilder()
      .addRuleInstance(FilterProjectTransposeRule.INSTANCE)
      .addRuleInstance(ProjectMergeRule.INSTANCE)
      .build();

  //~ Constructors -----------------------------------------------------------

  /** Creates a MaterializedViewFilterScanRule. */
  protected MaterializedViewFilterScanRule(RelBuilderFactory relBuilderFactory) {
    super(operand(Filter.class, operand(TableScan.class, null, none())),
        relBuilderFactory, "MaterializedViewFilterScanRule");
  }

  //~ Methods ----------------------------------------------------------------

  public void onMatch(RelOptRuleCall call) {
    final Filter filter = call.rel(0);
    final TableScan scan = call.rel(1);
    apply(call, filter, scan);
  }

  protected void apply(RelOptRuleCall call, Filter filter, TableScan scan) {
    RelOptPlanner planner = call.getPlanner();
    List<RelOptMaterialization> materializations =
        (planner instanceof VolcanoPlanner)
            ? ((VolcanoPlanner) planner).getMaterializations()
            : ImmutableList.<RelOptMaterialization>of();
    if (!materializations.isEmpty()) {
      RelNode root = filter.copy(filter.getTraitSet(),
          Collections.singletonList((RelNode) scan));
      List<RelOptMaterialization> applicableMaterializations =
          VolcanoPlanner.getApplicableMaterializations(root, materializations);
      for (RelOptMaterialization materialization : applicableMaterializations) {
        if (RelOptUtil.areRowTypesEqual(scan.getRowType(),
            materialization.queryRel.getRowType(), false)) {
          RelNode target = materialization.queryRel;
          final HepPlanner hepPlanner =
              new HepPlanner(program, planner.getContext());
          hepPlanner.setRoot(target);
          target = hepPlanner.findBestExp();
          List<RelNode> subs = new MaterializedViewSubstitutionVisitor(target, root)
              .go(materialization.tableRel);
          for (RelNode s : subs) {
            call.transformTo(s);
          }
        }
      }
    }
  }
}

// End MaterializedViewFilterScanRule.java