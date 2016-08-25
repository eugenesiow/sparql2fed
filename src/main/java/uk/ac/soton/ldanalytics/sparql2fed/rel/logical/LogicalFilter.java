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
import uk.ac.soton.ldanalytics.sparql2fed.plan.RelTraitSet;
import uk.ac.soton.ldanalytics.sparql2fed.rel.RelCollation;
import uk.ac.soton.ldanalytics.sparql2fed.rel.RelCollationTraitDef;
import uk.ac.soton.ldanalytics.sparql2fed.rel.RelDistribution;
import uk.ac.soton.ldanalytics.sparql2fed.rel.RelDistributionTraitDef;
import uk.ac.soton.ldanalytics.sparql2fed.rel.RelInput;
import uk.ac.soton.ldanalytics.sparql2fed.rel.RelNode;
import uk.ac.soton.ldanalytics.sparql2fed.rel.RelShuttle;
import uk.ac.soton.ldanalytics.sparql2fed.rel.RelWriter;
import uk.ac.soton.ldanalytics.sparql2fed.rel.core.CorrelationId;
import uk.ac.soton.ldanalytics.sparql2fed.rel.core.Filter;
import uk.ac.soton.ldanalytics.sparql2fed.rel.metadata.RelMdCollation;
import uk.ac.soton.ldanalytics.sparql2fed.rel.metadata.RelMdDistribution;
import uk.ac.soton.ldanalytics.sparql2fed.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;

import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.Set;

/**
 * Sub-class of {@link uk.ac.soton.ldanalytics.sparql2fed.rel.core.Filter}
 * not targeted at any particular engine or calling convention.
 */
public final class LogicalFilter extends Filter {
  private final ImmutableSet<CorrelationId> variablesSet;

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a LogicalFilter.
   *
   * <p>Use {@link #create} unless you know what you're doing.
   *
   * @param cluster   Cluster that this relational expression belongs to
   * @param child     Input relational expression
   * @param condition Boolean expression which determines whether a row is
   *                  allowed to pass
   * @param variablesSet Correlation variables set by this relational expression
   *                     to be used by nested expressions
   */
  public LogicalFilter(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelNode child,
      RexNode condition,
      ImmutableSet<CorrelationId> variablesSet) {
    super(cluster, traitSet, child, condition);
    this.variablesSet = Preconditions.checkNotNull(variablesSet);
  }

  @Deprecated // to be removed before 2.0
  public LogicalFilter(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelNode child,
      RexNode condition) {
    this(cluster, traitSet, child, condition, ImmutableSet.<CorrelationId>of());
  }

  @Deprecated // to be removed before 2.0
  public LogicalFilter(
      RelOptCluster cluster,
      RelNode child,
      RexNode condition) {
    this(cluster, cluster.traitSetOf(Convention.NONE), child, condition,
        ImmutableSet.<CorrelationId>of());
  }

  /**
   * Creates a LogicalFilter by parsing serialized output.
   */
  public LogicalFilter(RelInput input) {
    super(input);
    this.variablesSet = ImmutableSet.of();
  }

  /** Creates a LogicalFilter. */
  public static LogicalFilter create(final RelNode input, RexNode condition) {
    return create(input, condition, ImmutableSet.<CorrelationId>of());
  }

  /** Creates a LogicalFilter. */
  public static LogicalFilter create(final RelNode input, RexNode condition,
      ImmutableSet<CorrelationId> variablesSet) {
    final RelOptCluster cluster = input.getCluster();
    final RelMetadataQuery mq = RelMetadataQuery.instance();
    final RelTraitSet traitSet = cluster.traitSetOf(Convention.NONE)
        .replaceIfs(RelCollationTraitDef.INSTANCE,
            new Supplier<List<RelCollation>>() {
              public List<RelCollation> get() {
                return RelMdCollation.filter(mq, input);
              }
            })
        .replaceIf(RelDistributionTraitDef.INSTANCE,
            new Supplier<RelDistribution>() {
              public RelDistribution get() {
                return RelMdDistribution.filter(mq, input);
              }
            });
    return new LogicalFilter(cluster, traitSet, input, condition, variablesSet);
  }

  //~ Methods ----------------------------------------------------------------

  @Override public Set<CorrelationId> getVariablesSet() {
    return variablesSet;
  }

  public LogicalFilter copy(RelTraitSet traitSet, RelNode input,
      RexNode condition) {
    assert traitSet.containsIfApplicable(Convention.NONE);
    return new LogicalFilter(getCluster(), traitSet, input, condition,
        variablesSet);
  }

  @Override public RelNode accept(RelShuttle shuttle) {
    return shuttle.visit(this);
  }

  @Override public RelWriter explainTerms(RelWriter pw) {
    return super.explainTerms(pw)
        .itemIf("variablesSet", variablesSet, !variablesSet.isEmpty());
  }
}

// End LogicalFilter.java
