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

import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.materialize.Lattice;
import org.apache.calcite.materialize.TileKey;
import uk.ac.soton.ldanalytics.sparql2fed.plan.RelOptCluster;
import uk.ac.soton.ldanalytics.sparql2fed.plan.RelOptLattice;
import uk.ac.soton.ldanalytics.sparql2fed.plan.RelOptRule;
import uk.ac.soton.ldanalytics.sparql2fed.plan.RelOptRuleCall;
import uk.ac.soton.ldanalytics.sparql2fed.plan.RelOptRuleOperand;
import uk.ac.soton.ldanalytics.sparql2fed.plan.RelOptTable;
import uk.ac.soton.ldanalytics.sparql2fed.plan.RelOptUtil;
import uk.ac.soton.ldanalytics.sparql2fed.plan.SubstitutionVisitor;
import org.apache.calcite.prepare.CalcitePrepareImpl;
import org.apache.calcite.prepare.RelOptTableImpl;
import uk.ac.soton.ldanalytics.sparql2fed.rel.RelNode;
import uk.ac.soton.ldanalytics.sparql2fed.rel.core.Aggregate;
import uk.ac.soton.ldanalytics.sparql2fed.rel.core.AggregateCall;
import uk.ac.soton.ldanalytics.sparql2fed.rel.core.Project;
import uk.ac.soton.ldanalytics.sparql2fed.rel.metadata.RelMetadataQuery;
import uk.ac.soton.ldanalytics.sparql2fed.rel.type.RelDataType;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.StarTable;
import uk.ac.soton.ldanalytics.sparql2fed.sql.SqlAggFunction;
import org.apache.calcite.tools.RelBuilder;
import uk.ac.soton.ldanalytics.sparql2fed.util.ImmutableBitSet;
import uk.ac.soton.ldanalytics.sparql2fed.util.Pair;
import uk.ac.soton.ldanalytics.sparql2fed.util.mapping.AbstractSourceMapping;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.util.List;

/**
 * Planner rule that matches an {@link uk.ac.soton.ldanalytics.sparql2fed.rel.core.Aggregate} on
 * top of a {@link org.apache.calcite.schema.impl.StarTable.StarTableScan}.
 *
 * <p>This pattern indicates that an aggregate table may exist. The rule asks
 * the star table for an aggregate table at the required level of aggregation.
 */
public class AggregateStarTableRule extends RelOptRule {
  public static final AggregateStarTableRule INSTANCE =
      new AggregateStarTableRule(
          operand(Aggregate.class, null, Aggregate.IS_SIMPLE,
              some(operand(StarTable.StarTableScan.class, none()))),
          "AggregateStarTableRule");

  public static final AggregateStarTableRule INSTANCE2 =
      new AggregateStarTableRule(
          operand(Aggregate.class, null, Aggregate.IS_SIMPLE,
              operand(Project.class,
                  operand(StarTable.StarTableScan.class, none()))),
          "AggregateStarTableRule:project") {
        @Override public void onMatch(RelOptRuleCall call) {
          final Aggregate aggregate = call.rel(0);
          final Project project = call.rel(1);
          final StarTable.StarTableScan scan = call.rel(2);
          final RelNode rel =
              AggregateProjectMergeRule.apply(call, aggregate, project);
          final Aggregate aggregate2;
          final Project project2;
          if (rel instanceof Aggregate) {
            project2 = null;
            aggregate2 = (Aggregate) rel;
          } else if (rel instanceof Project) {
            project2 = (Project) rel;
            aggregate2 = (Aggregate) project2.getInput();
          } else {
            return;
          }
          apply(call, project2, aggregate2, scan);
        }
      };

  private AggregateStarTableRule(RelOptRuleOperand operand,
      String description) {
    super(operand, description);
  }

  @Override public void onMatch(RelOptRuleCall call) {
    final Aggregate aggregate = call.rel(0);
    final StarTable.StarTableScan scan = call.rel(1);
    apply(call, null, aggregate, scan);
  }

  protected void apply(RelOptRuleCall call, Project postProject,
      final Aggregate aggregate, StarTable.StarTableScan scan) {
    final RelOptCluster cluster = scan.getCluster();
    final RelOptTable table = scan.getTable();
    final RelOptLattice lattice = call.getPlanner().getLattice(table);
    final List<Lattice.Measure> measures =
        lattice.lattice.toMeasures(aggregate.getAggCallList());
    final Pair<CalciteSchema.TableEntry, TileKey> pair =
        lattice.getAggregate(
            call.getPlanner(), aggregate.getGroupSet(), measures);
    if (pair == null) {
      return;
    }
    final RelBuilder relBuilder = call.builder();
    final CalciteSchema.TableEntry tableEntry = pair.left;
    final TileKey tileKey = pair.right;
    final RelMetadataQuery mq = RelMetadataQuery.instance();
    final double rowCount = aggregate.estimateRowCount(mq);
    final Table aggregateTable = tableEntry.getTable();
    final RelDataType aggregateTableRowType =
        aggregateTable.getRowType(cluster.getTypeFactory());
    final RelOptTable aggregateRelOptTable =
        RelOptTableImpl.create(
            table.getRelOptSchema(),
            aggregateTableRowType,
            tableEntry,
            rowCount);
    relBuilder.push(aggregateRelOptTable.toRel(RelOptUtil.getContext(cluster)));
    if (tileKey == null) {
      if (CalcitePrepareImpl.DEBUG) {
        System.out.println("Using materialization "
            + aggregateRelOptTable.getQualifiedName()
            + " (exact match)");
      }
    } else if (!tileKey.dimensions.equals(aggregate.getGroupSet())) {
      // Aggregate has finer granularity than we need. Roll up.
      if (CalcitePrepareImpl.DEBUG) {
        System.out.println("Using materialization "
            + aggregateRelOptTable.getQualifiedName()
            + ", rolling up " + tileKey.dimensions + " to "
            + aggregate.getGroupSet());
      }
      assert tileKey.dimensions.contains(aggregate.getGroupSet());
      final List<AggregateCall> aggCalls = Lists.newArrayList();
      ImmutableBitSet.Builder groupSet = ImmutableBitSet.builder();
      for (int key : aggregate.getGroupSet()) {
        groupSet.set(tileKey.dimensions.indexOf(key));
      }
      for (AggregateCall aggCall : aggregate.getAggCallList()) {
        final AggregateCall copy =
            rollUp(groupSet.cardinality(), relBuilder, aggCall, tileKey);
        if (copy == null) {
          return;
        }
        aggCalls.add(copy);
      }
      relBuilder.push(
          aggregate.copy(aggregate.getTraitSet(), relBuilder.build(), false,
              groupSet.build(), null, aggCalls));
    } else if (!tileKey.measures.equals(measures)) {
      if (CalcitePrepareImpl.DEBUG) {
        System.out.println("Using materialization "
            + aggregateRelOptTable.getQualifiedName()
            + ", right granularity, but different measures "
            + aggregate.getAggCallList());
      }
      relBuilder.project(
          relBuilder.fields(
              new AbstractSourceMapping(
                  tileKey.dimensions.cardinality() + tileKey.measures.size(),
                  aggregate.getRowType().getFieldCount()) {
                public int getSourceOpt(int source) {
                  assert aggregate.getIndicatorCount() == 0;
                  if (source < aggregate.getGroupCount()) {
                    int in = tileKey.dimensions.nth(source);
                    return aggregate.getGroupSet().indexOf(in);
                  }
                  Lattice.Measure measure =
                      measures.get(source - aggregate.getGroupCount());
                  int i = tileKey.measures.indexOf(measure);
                  assert i >= 0;
                  return tileKey.dimensions.cardinality() + i;
                }
              } .inverse()));
    }
    if (postProject != null) {
      relBuilder.push(
          postProject.copy(postProject.getTraitSet(),
              ImmutableList.of(relBuilder.peek())));
    }
    call.transformTo(relBuilder.build());
  }

  private static AggregateCall rollUp(int groupCount, RelBuilder relBuilder,
      AggregateCall aggregateCall, TileKey tileKey) {
    if (aggregateCall.isDistinct()) {
      return null;
    }
    final SqlAggFunction aggregation = aggregateCall.getAggregation();
    final Pair<SqlAggFunction, List<Integer>> seek =
        Pair.of(aggregation, aggregateCall.getArgList());
    final int offset = tileKey.dimensions.cardinality();
    final ImmutableList<Lattice.Measure> measures = tileKey.measures;

    // First, try to satisfy the aggregation by rolling up an aggregate in the
    // materialization.
    final int i = find(measures, seek);
  tryRoll:
    if (i >= 0) {
      final SqlAggFunction roll = SubstitutionVisitor.getRollup(aggregation);
      if (roll == null) {
        break tryRoll;
      }
      return AggregateCall.create(roll, false, ImmutableList.of(offset + i), -1,
          groupCount, relBuilder.peek(), null, aggregateCall.name);
    }

    // Second, try to satisfy the aggregation based on group set columns.
  tryGroup:
    {
      List<Integer> newArgs = Lists.newArrayList();
      for (Integer arg : aggregateCall.getArgList()) {
        int z = tileKey.dimensions.indexOf(arg);
        if (z < 0) {
          break tryGroup;
        }
        newArgs.add(z);
      }
      return AggregateCall.create(aggregation, false, newArgs, -1,
          groupCount, relBuilder.peek(), null, aggregateCall.name);
    }

    // No roll up possible.
    return null;
  }

  private static int find(ImmutableList<Lattice.Measure> measures,
      Pair<SqlAggFunction, List<Integer>> seek) {
    for (int i = 0; i < measures.size(); i++) {
      Lattice.Measure measure = measures.get(i);
      if (measure.agg.equals(seek.left)
          && measure.argOrdinals().equals(seek.right)) {
        return i;
      }
    }
    return -1;
  }
}

// End AggregateStarTableRule.java
