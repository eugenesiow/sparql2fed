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
import uk.ac.soton.ldanalytics.sparql2fed.plan.RelOptTable;
import uk.ac.soton.ldanalytics.sparql2fed.plan.RelTraitSet;
import org.apache.calcite.prepare.Prepare;
import uk.ac.soton.ldanalytics.sparql2fed.rel.RelNode;
import uk.ac.soton.ldanalytics.sparql2fed.rel.core.TableModify;

import java.util.List;

/**
 * Sub-class of {@link uk.ac.soton.ldanalytics.sparql2fed.rel.core.TableModify}
 * not targeted at any particular engine or calling convention.
 */
public final class LogicalTableModify extends TableModify {
  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a LogicalTableModify.
   *
   * <p>Use {@link #create} unless you know what you're doing.
   */
  public LogicalTableModify(RelOptCluster cluster, RelTraitSet traitSet,
      RelOptTable table, Prepare.CatalogReader schema, RelNode input,
      Operation operation, List<String> updateColumnList, boolean flattened) {
    super(cluster, traitSet, table, schema, input, operation, updateColumnList,
        flattened);
  }

  @Deprecated // to be removed before 2.0
  public LogicalTableModify(RelOptCluster cluster, RelOptTable table,
      Prepare.CatalogReader schema, RelNode input, Operation operation,
      List<String> updateColumnList, boolean flattened) {
    this(cluster,
        cluster.traitSetOf(Convention.NONE),
        table,
        schema,
        input,
        operation,
        updateColumnList,
        flattened);
  }

  /** Creates a LogicalTableModify. */
  public static LogicalTableModify create(RelOptTable table,
      Prepare.CatalogReader schema, RelNode input,
      Operation operation, List<String> updateColumnList, boolean flattened) {
    final RelOptCluster cluster = input.getCluster();
    final RelTraitSet traitSet = cluster.traitSetOf(Convention.NONE);
    return new LogicalTableModify(cluster, traitSet, table, schema, input,
        operation, updateColumnList, flattened);
  }

  //~ Methods ----------------------------------------------------------------

  @Override public LogicalTableModify copy(RelTraitSet traitSet,
      List<RelNode> inputs) {
    assert traitSet.containsIfApplicable(Convention.NONE);
    return new LogicalTableModify(getCluster(), traitSet, table, catalogReader,
        sole(inputs), getOperation(), getUpdateColumnList(), isFlattened());
  }
}

// End LogicalTableModify.java