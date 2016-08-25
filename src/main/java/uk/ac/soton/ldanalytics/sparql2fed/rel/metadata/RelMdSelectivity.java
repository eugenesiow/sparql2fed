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
package uk.ac.soton.ldanalytics.sparql2fed.rel.metadata;

import uk.ac.soton.ldanalytics.sparql2fed.plan.RelOptUtil;
import uk.ac.soton.ldanalytics.sparql2fed.rel.RelNode;
import uk.ac.soton.ldanalytics.sparql2fed.rel.core.Aggregate;
import uk.ac.soton.ldanalytics.sparql2fed.rel.core.Filter;
import uk.ac.soton.ldanalytics.sparql2fed.rel.core.Project;
import uk.ac.soton.ldanalytics.sparql2fed.rel.core.SemiJoin;
import uk.ac.soton.ldanalytics.sparql2fed.rel.core.Sort;
import uk.ac.soton.ldanalytics.sparql2fed.rel.core.Union;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import uk.ac.soton.ldanalytics.sparql2fed.sql.fun.SqlStdOperatorTable;
import uk.ac.soton.ldanalytics.sparql2fed.util.BuiltInMethod;
import uk.ac.soton.ldanalytics.sparql2fed.util.ImmutableBitSet;

import java.util.ArrayList;
import java.util.List;

/**
 * RelMdSelectivity supplies a default implementation of
 * {@link RelMetadataQuery#getSelectivity} for the standard logical algebra.
 */
public class RelMdSelectivity
    implements MetadataHandler<BuiltInMetadata.Selectivity> {
  public static final RelMetadataProvider SOURCE =
      ReflectiveRelMetadataProvider.reflectiveSource(
          BuiltInMethod.SELECTIVITY.method, new RelMdSelectivity());

  //~ Constructors -----------------------------------------------------------

  protected RelMdSelectivity() {
  }

  //~ Methods ----------------------------------------------------------------

  public MetadataDef<BuiltInMetadata.Selectivity> getDef() {
    return BuiltInMetadata.Selectivity.DEF;
  }

  public Double getSelectivity(Union rel, RelMetadataQuery mq,
      RexNode predicate) {
    if ((rel.getInputs().size() == 0) || (predicate == null)) {
      return 1.0;
    }

    double sumRows = 0.0;
    double sumSelectedRows = 0.0;
    int[] adjustments = new int[rel.getRowType().getFieldCount()];
    RexBuilder rexBuilder = rel.getCluster().getRexBuilder();
    for (RelNode input : rel.getInputs()) {
      Double nRows = mq.getRowCount(input);
      if (nRows == null) {
        return null;
      }

      // convert the predicate to reference the types of the union child
      RexNode modifiedPred =
          predicate.accept(
              new RelOptUtil.RexInputConverter(
                  rexBuilder,
                  null,
                  input.getRowType().getFieldList(),
                  adjustments));
      double sel = mq.getSelectivity(input, modifiedPred);

      sumRows += nRows;
      sumSelectedRows += nRows * sel;
    }

    if (sumRows < 1.0) {
      sumRows = 1.0;
    }
    return sumSelectedRows / sumRows;
  }

  public Double getSelectivity(Sort rel, RelMetadataQuery mq,
      RexNode predicate) {
    return mq.getSelectivity(rel.getInput(), predicate);
  }

  public Double getSelectivity(Filter rel, RelMetadataQuery mq,
      RexNode predicate) {
    // Take the difference between the predicate passed in and the
    // predicate in the filter's condition, so we don't apply the
    // selectivity of the filter twice.  If no predicate is passed in,
    // use the filter's condition.
    if (predicate != null) {
      return mq.getSelectivity(rel.getInput(),
          RelMdUtil.minusPreds(
              rel.getCluster().getRexBuilder(),
              predicate,
              rel.getCondition()));
    } else {
      return mq.getSelectivity(rel.getInput(), rel.getCondition());
    }
  }

  public Double getSelectivity(SemiJoin rel, RelMetadataQuery mq,
      RexNode predicate) {
    // create a RexNode representing the selectivity of the
    // semijoin filter and pass it to getSelectivity
    RexBuilder rexBuilder = rel.getCluster().getRexBuilder();
    RexNode newPred = RelMdUtil.makeSemiJoinSelectivityRexNode(mq, rel);
    if (predicate != null) {
      newPred =
          rexBuilder.makeCall(
              SqlStdOperatorTable.AND,
              newPred,
              predicate);
    }

    return mq.getSelectivity(rel.getLeft(), newPred);
  }

  public Double getSelectivity(Aggregate rel, RelMetadataQuery mq,
      RexNode predicate) {
    final List<RexNode> notPushable = new ArrayList<>();
    final List<RexNode> pushable = new ArrayList<>();
    RelOptUtil.splitFilters(
        rel.getGroupSet(),
        predicate,
        pushable,
        notPushable);
    final RexBuilder rexBuilder = rel.getCluster().getRexBuilder();
    RexNode childPred =
        RexUtil.composeConjunction(rexBuilder, pushable, true);

    Double selectivity = mq.getSelectivity(rel.getInput(), childPred);
    if (selectivity == null) {
      return null;
    } else {
      RexNode pred =
          RexUtil.composeConjunction(rexBuilder, notPushable, true);
      return selectivity * RelMdUtil.guessSelectivity(pred);
    }
  }

  public Double getSelectivity(Project rel, RelMetadataQuery mq,
      RexNode predicate) {
    final List<RexNode> notPushable = new ArrayList<>();
    final List<RexNode> pushable = new ArrayList<>();
    RelOptUtil.splitFilters(
        ImmutableBitSet.range(rel.getRowType().getFieldCount()),
        predicate,
        pushable,
        notPushable);
    final RexBuilder rexBuilder = rel.getCluster().getRexBuilder();
    RexNode childPred =
        RexUtil.composeConjunction(rexBuilder, pushable, true);

    RexNode modifiedPred;
    if (childPred == null) {
      modifiedPred = null;
    } else {
      modifiedPred = RelOptUtil.pushPastProject(childPred, rel);
    }
    Double selectivity = mq.getSelectivity(rel.getInput(), modifiedPred);
    if (selectivity == null) {
      return null;
    } else {
      RexNode pred =
          RexUtil.composeConjunction(rexBuilder, notPushable, true);
      return selectivity * RelMdUtil.guessSelectivity(pred);
    }
  }

  // Catch-all rule when none of the others apply.
  public Double getSelectivity(RelNode rel, RelMetadataQuery mq,
      RexNode predicate) {
    return RelMdUtil.guessSelectivity(predicate);
  }
}

// End RelMdSelectivity.java
