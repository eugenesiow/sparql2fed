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
package uk.ac.soton.ldanalytics.sparql2fed.rel.logical;

import uk.ac.soton.ldanalytics.sparql2fed.plan.Convention;
import uk.ac.soton.ldanalytics.sparql2fed.plan.RelOptCluster;
import uk.ac.soton.ldanalytics.sparql2fed.plan.RelOptUtil;
import uk.ac.soton.ldanalytics.sparql2fed.plan.RelTraitSet;
import uk.ac.soton.ldanalytics.sparql2fed.rel.RelCollation;
import uk.ac.soton.ldanalytics.sparql2fed.rel.RelCollationTraitDef;
import uk.ac.soton.ldanalytics.sparql2fed.rel.RelDistribution;
import uk.ac.soton.ldanalytics.sparql2fed.rel.RelDistributionTraitDef;
import uk.ac.soton.ldanalytics.sparql2fed.rel.RelNode;
import uk.ac.soton.ldanalytics.sparql2fed.rel.core.Calc;
import uk.ac.soton.ldanalytics.sparql2fed.rel.core.CorrelationId;
import uk.ac.soton.ldanalytics.sparql2fed.rel.metadata.RelMdCollation;
import uk.ac.soton.ldanalytics.sparql2fed.rel.metadata.RelMdDistribution;
import uk.ac.soton.ldanalytics.sparql2fed.rel.metadata.RelMetadataQuery;
import uk.ac.soton.ldanalytics.sparql2fed.rel.rules.FilterToCalcRule;
import uk.ac.soton.ldanalytics.sparql2fed.rel.rules.ProjectToCalcRule;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import uk.ac.soton.ldanalytics.sparql2fed.util.Util;

import com.google.common.base.Supplier;

import java.util.List;
import java.util.Set;

/**
 * A relational expression which computes project expressions and also filters.
 *
 * <p>This relational expression combines the functionality of
 * {@link LogicalProject} and {@link LogicalFilter}.
 * It should be created in the later
 * stages of optimization, by merging consecutive {@link LogicalProject} and
 * {@link LogicalFilter} nodes together.
 *
 * <p>The following rules relate to <code>LogicalCalc</code>:</p>
 *
 * <ul>
 * <li>{@link FilterToCalcRule} creates this from a {@link LogicalFilter}
 * <li>{@link ProjectToCalcRule} creates this from a {@link LogicalFilter}
 * <li>{@link uk.ac.soton.ldanalytics.sparql2fed.rel.rules.FilterCalcMergeRule}
 *     merges this with a {@link LogicalFilter}
 * <li>{@link uk.ac.soton.ldanalytics.sparql2fed.rel.rules.ProjectCalcMergeRule}
 *     merges this with a {@link LogicalProject}
 * <li>{@link uk.ac.soton.ldanalytics.sparql2fed.rel.rules.CalcMergeRule}
 *     merges two {@code LogicalCalc}s
 * </ul>
 */
public final class LogicalCalc extends Calc {
  //~ Static fields/initializers ---------------------------------------------

  //~ Constructors -----------------------------------------------------------

  /** Creates a LogicalCalc. */
  public LogicalCalc(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelNode child,
      RexProgram program) {
    super(cluster, traitSet, child, program);
  }

  @Deprecated // to be removed before 2.0
  public LogicalCalc(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelNode child,
      RexProgram program,
      List<RelCollation> collationList) {
    this(cluster, traitSet, child, program);
    Util.discard(collationList);
  }

  public static LogicalCalc create(final RelNode input,
      final RexProgram program) {
    final RelOptCluster cluster = input.getCluster();
    final RelMetadataQuery mq = RelMetadataQuery.instance();
    final RelTraitSet traitSet = cluster.traitSet()
        .replace(Convention.NONE)
        .replaceIfs(RelCollationTraitDef.INSTANCE,
            new Supplier<List<RelCollation>>() {
              public List<RelCollation> get() {
                return RelMdCollation.calc(mq, input, program);
              }
            })
        .replaceIf(RelDistributionTraitDef.INSTANCE,
            new Supplier<RelDistribution>() {
              public RelDistribution get() {
                return RelMdDistribution.calc(mq, input, program);
              }
            });
    return new LogicalCalc(cluster, traitSet, input, program);
  }

  //~ Methods ----------------------------------------------------------------

  @Override public LogicalCalc copy(RelTraitSet traitSet, RelNode child,
      RexProgram program) {
    return new LogicalCalc(getCluster(), traitSet, child, program);
  }

  @Override public void collectVariablesUsed(Set<CorrelationId> variableSet) {
    final RelOptUtil.VariableUsedVisitor vuv =
        new RelOptUtil.VariableUsedVisitor(null);
    for (RexNode expr : program.getExprList()) {
      expr.accept(vuv);
    }
    variableSet.addAll(vuv.variables);
  }
}

// End LogicalCalc.java
