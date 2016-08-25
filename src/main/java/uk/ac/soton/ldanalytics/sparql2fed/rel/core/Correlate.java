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
import uk.ac.soton.ldanalytics.sparql2fed.plan.RelOptUtil;
import uk.ac.soton.ldanalytics.sparql2fed.plan.RelTraitSet;
import uk.ac.soton.ldanalytics.sparql2fed.rel.BiRel;
import uk.ac.soton.ldanalytics.sparql2fed.rel.RelInput;
import uk.ac.soton.ldanalytics.sparql2fed.rel.RelNode;
import uk.ac.soton.ldanalytics.sparql2fed.rel.RelWriter;
import uk.ac.soton.ldanalytics.sparql2fed.rel.logical.LogicalJoin;
import uk.ac.soton.ldanalytics.sparql2fed.rel.metadata.RelMetadataQuery;
import uk.ac.soton.ldanalytics.sparql2fed.rel.type.RelDataType;
import uk.ac.soton.ldanalytics.sparql2fed.sql.SemiJoinType;
import uk.ac.soton.ldanalytics.sparql2fed.util.ImmutableBitSet;
import uk.ac.soton.ldanalytics.sparql2fed.util.Litmus;

import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.Set;

/**
 * A relational operator that performs nested-loop joins.
 *
 * <p>It behaves like a kind of {@link uk.ac.soton.ldanalytics.sparql2fed.rel.core.Join},
 * but works by setting variables in its environment and restarting its
 * right-hand input.
 *
 * <p>Correlate is not a join since: typical rules should not match Correlate.
 *
 * <p>A Correlate is used to represent a correlated query. One
 * implementation strategy is to de-correlate the expression.
 *
 * <table>
 *   <caption>Mapping of physical operations to logical ones</caption>
 *   <tr><th>Physical operation</th><th>Logical operation</th></tr>
 *   <tr><td>NestedLoops</td><td>Correlate(A, B, regular)</td></tr>
 *   <tr><td>NestedLoopsOuter</td><td>Correlate(A, B, outer)</td></tr>
 *   <tr><td>NestedLoopsSemi</td><td>Correlate(A, B, semi)</td></tr>
 *   <tr><td>NestedLoopsAnti</td><td>Correlate(A, B, anti)</td></tr>
 *   <tr><td>HashJoin</td><td>EquiJoin(A, B)</td></tr>
 *   <tr><td>HashJoinOuter</td><td>EquiJoin(A, B, outer)</td></tr>
 *   <tr><td>HashJoinSemi</td><td>SemiJoin(A, B, semi)</td></tr>
 *   <tr><td>HashJoinAnti</td><td>SemiJoin(A, B, anti)</td></tr>
 * </table>
 *
 * @see CorrelationId
 */
public abstract class Correlate extends BiRel {
  //~ Instance fields --------------------------------------------------------

  protected final CorrelationId correlationId;
  protected final ImmutableBitSet requiredColumns;
  protected final SemiJoinType joinType;

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a Correlate.
   * @param cluster      Cluster this relational expression belongs to
   * @param left         Left input relational expression
   * @param right        Right input relational expression
   * @param correlationId Variable name for the row of left input
   * @param requiredColumns Set of columns that are used by correlation
   * @param joinType Join type
   */
  protected Correlate(
      RelOptCluster cluster,
      RelTraitSet traits,
      RelNode left,
      RelNode right,
      CorrelationId correlationId,
      ImmutableBitSet requiredColumns, SemiJoinType joinType) {
    super(cluster, traits, left, right);
    this.joinType = joinType;
    this.correlationId = correlationId;
    this.requiredColumns = requiredColumns;
  }

  /**
   * Creates a Correlate by parsing serialized output.
   *
   * @param input Input representation
   */
  public Correlate(RelInput input) {
    this(
        input.getCluster(), input.getTraitSet(), input.getInputs().get(0),
        input.getInputs().get(1),
        new CorrelationId((Integer) input.get("correlationId")),
        input.getBitSet("requiredColumns"),
        input.getEnum("joinType", SemiJoinType.class));
  }

  //~ Methods ----------------------------------------------------------------

  @Override public boolean isValid(Litmus litmus) {
    return super.isValid(litmus)
        && RelOptUtil.notContainsCorrelation(left, correlationId, litmus);
  }

  @Override public Correlate copy(RelTraitSet traitSet, List<RelNode> inputs) {
    assert inputs.size() == 2;
    return copy(traitSet,
        inputs.get(0),
        inputs.get(1),
        correlationId,
        requiredColumns,
        joinType);
  }

  public abstract Correlate copy(RelTraitSet traitSet,
      RelNode left, RelNode right, CorrelationId correlationId,
      ImmutableBitSet requiredColumns, SemiJoinType joinType);

  public SemiJoinType getJoinType() {
    return joinType;
  }

  @Override protected RelDataType deriveRowType() {
    switch (joinType) {
    case LEFT:
    case INNER:
      // LogicalJoin is used to share the code of column names deduplication
      final LogicalJoin join = LogicalJoin.create(left, right,
          getCluster().getRexBuilder().makeLiteral(true),
          ImmutableSet.<CorrelationId>of(), joinType.toJoinType());
      return join.deriveRowType();
    case ANTI:
    case SEMI:
      return left.getRowType();
    default:
      throw new IllegalStateException("Unknown join type " + joinType);
    }
  }

  @Override public RelWriter explainTerms(RelWriter pw) {
    return super.explainTerms(pw)
        .item("correlation", correlationId)
        .item("joinType", joinType)
        .item("requiredColumns", requiredColumns.toString());
  }

  /**
   * Returns the correlating expressions.
   *
   * @return correlating expressions
   */
  public CorrelationId getCorrelationId() {
    return correlationId;
  }

  @Override public String getCorrelVariable() {
    return correlationId.getName();
  }

  /**
   * Returns the required columns in left relation required for the correlation
   * in the right.
   *
   * @return columns in left relation required for the correlation in the right
   */
  public ImmutableBitSet getRequiredColumns() {
    return requiredColumns;
  }

  @Override public Set<CorrelationId> getVariablesSet() {
    return ImmutableSet.of(correlationId);
  }

  @Override public RelOptCost computeSelfCost(RelOptPlanner planner,
      RelMetadataQuery mq) {
    double rowCount = mq.getRowCount(this);

    final double rightRowCount = right.estimateRowCount(mq);
    final double leftRowCount = left.estimateRowCount(mq);
    if (Double.isInfinite(leftRowCount) || Double.isInfinite(rightRowCount)) {
      return planner.getCostFactory().makeInfiniteCost();
    }

    Double restartCount = mq.getRowCount(getLeft());
    // RelMetadataQuery.getCumulativeCost(getRight()); does not work for
    // RelSubset, so we ask planner to cost-estimate right relation
    RelOptCost rightCost = planner.getCost(getRight(), mq);
    RelOptCost rescanCost =
        rightCost.multiplyBy(Math.max(1.0, restartCount - 1));

    return planner.getCostFactory().makeCost(
        rowCount /* generate results */ + leftRowCount /* scan left results */,
        0, 0).plus(rescanCost);
  }
}

// End Correlate.java
