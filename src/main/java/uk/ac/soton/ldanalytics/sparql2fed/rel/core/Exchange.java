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
package uk.ac.soton.ldanalytics.sparql2fed.rel.core;

import uk.ac.soton.ldanalytics.sparql2fed.plan.RelOptCluster;
import uk.ac.soton.ldanalytics.sparql2fed.plan.RelOptCost;
import uk.ac.soton.ldanalytics.sparql2fed.plan.RelOptPlanner;
import uk.ac.soton.ldanalytics.sparql2fed.plan.RelTraitSet;
import uk.ac.soton.ldanalytics.sparql2fed.rel.RelDistribution;
import uk.ac.soton.ldanalytics.sparql2fed.rel.RelDistributionTraitDef;
import uk.ac.soton.ldanalytics.sparql2fed.rel.RelDistributions;
import uk.ac.soton.ldanalytics.sparql2fed.rel.RelInput;
import uk.ac.soton.ldanalytics.sparql2fed.rel.RelNode;
import uk.ac.soton.ldanalytics.sparql2fed.rel.RelWriter;
import uk.ac.soton.ldanalytics.sparql2fed.rel.SingleRel;
import uk.ac.soton.ldanalytics.sparql2fed.rel.metadata.RelMetadataQuery;
import uk.ac.soton.ldanalytics.sparql2fed.util.Util;

import com.google.common.base.Preconditions;

import java.util.List;

/**
 * Relational expression that imposes a particular distribution on its input
 * without otherwise changing its content.
 *
 * @see uk.ac.soton.ldanalytics.sparql2fed.rel.core.SortExchange
 */
public abstract class Exchange extends SingleRel {
  //~ Instance fields --------------------------------------------------------

  public final RelDistribution distribution;

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates an Exchange.
   *
   * @param cluster   Cluster this relational expression belongs to
   * @param traitSet  Trait set
   * @param input     Input relational expression
   * @param distribution Distribution specification
   */
  protected Exchange(RelOptCluster cluster, RelTraitSet traitSet, RelNode input,
      RelDistribution distribution) {
    super(cluster, traitSet, input);
    this.distribution = Preconditions.checkNotNull(distribution);

    assert traitSet.containsIfApplicable(distribution)
        : "traits=" + traitSet + ", distribution" + distribution;
    assert distribution != RelDistributions.ANY;
  }

  /**
   * Creates a Exchange by parsing serialized output.
   */
  public Exchange(RelInput input) {
    this(input.getCluster(), input.getTraitSet().plus(input.getCollation()),
        input.getInput(),
        RelDistributionTraitDef.INSTANCE.canonize(input.getDistribution()));
  }

  //~ Methods ----------------------------------------------------------------

  @Override public final Exchange copy(RelTraitSet traitSet,
      List<RelNode> inputs) {
    return copy(traitSet, sole(inputs), distribution);
  }

  public abstract Exchange copy(RelTraitSet traitSet, RelNode newInput,
      RelDistribution newDistribution);

  /** Returns the distribution of the rows returned by this Exchange. */
  public RelDistribution getDistribution() {
    return distribution;
  }

  @Override public RelOptCost computeSelfCost(RelOptPlanner planner,
      RelMetadataQuery mq) {
    // Higher cost if rows are wider discourages pushing a project through an
    // exchange.
    double rowCount = mq.getRowCount(this);
    double bytesPerRow = getRowType().getFieldCount() * 4;
    return planner.getCostFactory().makeCost(
        Util.nLogN(rowCount) * bytesPerRow, rowCount, 0);
  }

  public RelWriter explainTerms(RelWriter pw) {
    return super.explainTerms(pw)
        .item("distribution", distribution);
  }
}

// End Exchange.java
