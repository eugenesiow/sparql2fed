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
import uk.ac.soton.ldanalytics.sparql2fed.rel.RelCollations;
import uk.ac.soton.ldanalytics.sparql2fed.rel.RelInput;
import uk.ac.soton.ldanalytics.sparql2fed.rel.RelNode;
import uk.ac.soton.ldanalytics.sparql2fed.rel.RelShuttle;
import uk.ac.soton.ldanalytics.sparql2fed.rel.core.Project;
import uk.ac.soton.ldanalytics.sparql2fed.rel.metadata.RelMdCollation;
import uk.ac.soton.ldanalytics.sparql2fed.rel.metadata.RelMetadataQuery;
import uk.ac.soton.ldanalytics.sparql2fed.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import uk.ac.soton.ldanalytics.sparql2fed.sql.validate.SqlValidatorUtil;
import uk.ac.soton.ldanalytics.sparql2fed.util.Util;

import com.google.common.base.Supplier;

import java.util.List;

/**
 * Sub-class of {@link uk.ac.soton.ldanalytics.sparql2fed.rel.core.Project} not
 * targeted at any particular engine or calling convention.
 */
public final class LogicalProject extends Project {
  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a LogicalProject.
   *
   * <p>Use {@link #create} unless you know what you're doing.
   *
   * @param cluster  Cluster this relational expression belongs to
   * @param traitSet Traits of this relational expression
   * @param input    Input relational expression
   * @param projects List of expressions for the input columns
   * @param rowType  Output row type
   */
  public LogicalProject(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelNode input,
      List<? extends RexNode> projects,
      RelDataType rowType) {
    super(cluster, traitSet, input, projects, rowType);
    assert traitSet.containsIfApplicable(Convention.NONE);
  }

  @Deprecated // to be removed before 2.0
  public LogicalProject(RelOptCluster cluster, RelTraitSet traitSet,
      RelNode input, List<? extends RexNode> projects, RelDataType rowType,
      int flags) {
    this(cluster, traitSet, input, projects, rowType);
    Util.discard(flags);
  }

  @Deprecated // to be removed before 2.0
  public LogicalProject(RelOptCluster cluster, RelNode input,
      List<RexNode> projects, List<String> fieldNames, int flags) {
    this(cluster, cluster.traitSetOf(RelCollations.EMPTY),
        input, projects,
        RexUtil.createStructType(cluster.getTypeFactory(), projects,
            fieldNames));
    Util.discard(flags);
  }

  /**
   * Creates a LogicalProject by parsing serialized output.
   */
  public LogicalProject(RelInput input) {
    super(input);
  }

  //~ Methods ----------------------------------------------------------------

  /** Creates a LogicalProject. */
  public static LogicalProject create(final RelNode input,
      final List<? extends RexNode> projects, List<String> fieldNames) {
    final RelOptCluster cluster = input.getCluster();
    final List<String> fieldNames2 =
        fieldNames == null
            ? null
            : SqlValidatorUtil.uniquify(fieldNames,
                SqlValidatorUtil.F_SUGGESTER);
    final RelDataType rowType =
        RexUtil.createStructType(cluster.getTypeFactory(), projects,
            fieldNames2);
    return create(input, projects, rowType);
  }

  /** Creates a LogicalProject, specifying row type rather than field names. */
  public static LogicalProject create(final RelNode input,
      final List<? extends RexNode> projects, RelDataType rowType) {
    final RelOptCluster cluster = input.getCluster();
    final RelMetadataQuery mq = RelMetadataQuery.instance();
    final RelTraitSet traitSet =
        cluster.traitSet().replace(Convention.NONE)
            .replaceIfs(
                RelCollationTraitDef.INSTANCE,
                new Supplier<List<RelCollation>>() {
                  public List<RelCollation> get() {
                    return RelMdCollation.project(mq, input, projects);
                  }
                });
    return new LogicalProject(cluster, traitSet, input, projects, rowType);
  }

  @Override public LogicalProject copy(RelTraitSet traitSet, RelNode input,
      List<RexNode> projects, RelDataType rowType) {
    return new LogicalProject(getCluster(), traitSet, input, projects, rowType);
  }

  @Override public RelNode accept(RelShuttle shuttle) {
    return shuttle.visit(this);
  }
}

// End LogicalProject.java
